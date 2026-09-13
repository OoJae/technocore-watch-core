// File-backed state: <dataDir>/v1/<sha256(scopeKey)[:16]>/state.json + notices.jsonl,
// and <dataDir>/v1/poller.lock for the single-poller rule.
//
// Every write is tmp file + fsync + rename + directory fsync, under a short-lived per-scope
// mutex file, with a revision compare-and-set so concurrent processes (a `run` daemon and
// an `inbox` ack) never lose each other's updates silently.
import { createHash, randomBytes } from 'node:crypto';
import { constants as FS } from 'node:fs';
import * as fs from 'node:fs/promises';
import { join } from 'node:path';
import type { Notice, WatchStateV1 } from '../protocol/types.ts';
import { MAX_STATE_BYTES, RevisionConflictError, StateError, migrateState } from './model.ts';
import type { LockInfo, StateStore, StoreLock } from './store.ts';

export const MAX_NOTICES_BYTES = 256 << 10;
const MUTEX_STALE_MS = 10_000;
const HEARTBEAT_MS = 10_000;
const LOCK_STALE_MS = 3 * HEARTBEAT_MS;

export function scopeHash(scopeKey: string): string {
  return createHash('sha256').update(scopeKey, 'utf8').digest('hex').slice(0, 16);
}

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function fsyncDir(dir: string): Promise<void> {
  let h: fs.FileHandle | undefined;
  try {
    h = await fs.open(dir, 'r');
    await h.sync();
  } catch {
    // Some platforms refuse fsync on directories; the rename is still atomic.
  } finally {
    await h?.close();
  }
}

/** Write `data` to `path` atomically: tmp + fsync + rename + dir fsync. */
export async function atomicWrite(path: string, data: string | Uint8Array, dir: string): Promise<void> {
  const tmp = `${path}.${process.pid}.${randomBytes(4).toString('hex')}.tmp`;
  const h = await fs.open(tmp, FS.O_WRONLY | FS.O_CREAT | FS.O_EXCL, 0o600);
  try {
    await h.writeFile(data);
    await h.sync();
  } finally {
    await h.close();
  }
  try {
    await fs.rename(tmp, path);
  } catch (e) {
    await fs.rm(tmp, { force: true });
    throw e;
  }
  await fsyncDir(dir);
}

function pidAlive(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return (e as NodeJS.ErrnoException).code === 'EPERM';
  }
}

export class FileStateStore implements StateStore {
  readonly dataDir: string;
  readonly root: string;
  private readonly heartbeatMs: number;
  private readonly staleMs: number;

  constructor(dataDir: string, o: { heartbeatMs?: number; staleAfterMs?: number } = {}) {
    this.dataDir = dataDir;
    this.root = join(dataDir, 'v1');
    this.heartbeatMs = o.heartbeatMs ?? HEARTBEAT_MS;
    this.staleMs = o.staleAfterMs ?? Math.max(LOCK_STALE_MS, 3 * this.heartbeatMs);
  }

  scopeDir(scopeKey: string): string {
    return join(this.root, scopeHash(scopeKey));
  }

  private statePath(scopeKey: string): string {
    return join(this.scopeDir(scopeKey), 'state.json');
  }

  noticesPath(scopeKey: string): string {
    return join(this.scopeDir(scopeKey), 'notices.jsonl');
  }

  private async ensureDir(dir: string): Promise<void> {
    await fs.mkdir(dir, { recursive: true, mode: 0o700 });
  }

  /** Short critical section across processes: O_EXCL mutex file, stale after 10 s. */
  private async withMutex<T>(dir: string, fn: () => Promise<T>): Promise<T> {
    await this.ensureDir(dir);
    const path = join(dir, '.mutex');
    const token = `${process.pid}:${randomBytes(6).toString('hex')}`;
    for (let attempt = 0; ; attempt++) {
      try {
        const h = await fs.open(path, FS.O_WRONLY | FS.O_CREAT | FS.O_EXCL, 0o600);
        await h.writeFile(token);
        await h.close();
        break;
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code !== 'EEXIST') throw e;
        try {
          const st = await fs.stat(path);
          if (Date.now() - st.mtimeMs > MUTEX_STALE_MS) {
            await fs.rm(path, { force: true });
            continue;
          }
        } catch {
          continue;
        }
        if (attempt > 2000) throw new StateError('timed out waiting for state mutex');
        await delay(Math.min(50, 2 + attempt));
      }
    }
    try {
      return await fn();
    } finally {
      try {
        if ((await fs.readFile(path, 'utf8')) === token) await fs.rm(path, { force: true });
      } catch {
        // already gone
      }
    }
  }

  private async readState(scopeKey: string): Promise<WatchStateV1 | null> {
    let text: string;
    try {
      text = await fs.readFile(this.statePath(scopeKey), 'utf8');
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw e;
    }
    if (text.length > MAX_STATE_BYTES * 4) throw new StateError('state file is far larger than its bound');
    return migrateState(JSON.parse(text));
  }

  async load(scopeKey: string): Promise<WatchStateV1 | null> {
    const s = await this.readState(scopeKey);
    if (s && s.scopeKey !== scopeKey) throw new StateError('state file belongs to a different scope');
    return s;
  }

  async revision(scopeKey: string): Promise<number> {
    try {
      return (await this.readState(scopeKey))?.revision ?? 0;
    } catch {
      return -1;
    }
  }

  async save(state: WatchStateV1, expectedRevision: number): Promise<WatchStateV1> {
    const dir = this.scopeDir(state.scopeKey);
    return this.withMutex(dir, async () => {
      const current = (await this.readState(state.scopeKey))?.revision ?? 0;
      if (current !== expectedRevision) throw new RevisionConflictError(expectedRevision, current);
      const stored: WatchStateV1 = { ...state, revision: expectedRevision + 1, updatedAt: new Date().toISOString() };
      const text = JSON.stringify(stored, null, 1) + '\n';
      if (Buffer.byteLength(text) > MAX_STATE_BYTES) throw new StateError('state exceeds 256 KiB');
      await atomicWrite(this.statePath(state.scopeKey), text, dir);
      return structuredClone(stored);
    });
  }

  async appendNotice(scopeKey: string, notice: Notice): Promise<void> {
    const dir = this.scopeDir(scopeKey);
    await this.withMutex(dir, async () => {
      const path = this.noticesPath(scopeKey);
      const line = JSON.stringify(notice) + '\n';
      let size = 0;
      try {
        size = (await fs.stat(path)).size;
      } catch {
        size = 0;
      }
      if (size + line.length > MAX_NOTICES_BYTES) {
        // Bounded ring: keep the newest half, rewritten atomically.
        let existing = '';
        try {
          existing = await fs.readFile(path, 'utf8');
        } catch {
          existing = '';
        }
        const keep = existing.slice(-Math.floor(MAX_NOTICES_BYTES / 2));
        const firstNl = keep.indexOf('\n');
        const tail = firstNl >= 0 ? keep.slice(firstNl + 1) : '';
        await atomicWrite(path, tail + line, dir);
        return;
      }
      const h = await fs.open(path, FS.O_WRONLY | FS.O_CREAT | FS.O_APPEND, 0o600);
      try {
        await h.writeFile(line);
        await h.sync();
      } finally {
        await h.close();
      }
    });
  }

  async readNotices(scopeKey: string): Promise<Notice[]> {
    let text = '';
    try {
      text = await fs.readFile(this.noticesPath(scopeKey), 'utf8');
    } catch {
      return [];
    }
    const out: Notice[] = [];
    for (const line of text.split('\n')) {
      if (!line.trim()) continue;
      try {
        out.push(JSON.parse(line) as Notice);
      } catch {
        // torn line
      }
    }
    return out;
  }

  async listScopes(): Promise<string[]> {
    let entries: string[] = [];
    try {
      entries = await fs.readdir(this.root);
    } catch {
      return [];
    }
    const scopes: string[] = [];
    for (const name of entries) {
      if (!/^[0-9a-f]{16}$/.test(name)) continue;
      try {
        const text = await fs.readFile(join(this.root, name, 'state.json'), 'utf8');
        const s = migrateState(JSON.parse(text));
        if (scopeHash(s.scopeKey) === name) scopes.push(s.scopeKey);
      } catch {
        // skip unreadable scope
      }
    }
    return scopes.sort();
  }

  async acquireLock(name = 'poller'): Promise<StoreLock | null> {
    if (!/^[a-z0-9-]{1,32}$/.test(name)) throw new TypeError('invalid lock name');
    await this.ensureDir(this.root);
    const path = join(this.root, `${name}.lock`);
    for (let attempt = 0; attempt < 3; attempt++) {
      const now = new Date().toISOString();
      const info: LockInfo = { pid: process.pid, startedAt: now, heartbeatAt: now, owner: randomBytes(6).toString('hex') };
      try {
        const h = await fs.open(path, FS.O_WRONLY | FS.O_CREAT | FS.O_EXCL, 0o600);
        try {
          await h.writeFile(JSON.stringify(info));
          await h.sync();
        } finally {
          await h.close();
        }
        return this.heldLock(path, info);
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code !== 'EEXIST') throw e;
      }
      let existingText: string;
      try {
        existingText = await fs.readFile(path, 'utf8');
      } catch {
        continue; // released between our open and read
      }
      let existing: LockInfo | null = null;
      try {
        existing = JSON.parse(existingText) as LockInfo;
      } catch {
        existing = null;
      }
      const beat = existing ? Date.parse(existing.heartbeatAt) : NaN;
      const stale = !existing || !Number.isFinite(beat) || Date.now() - beat > this.staleMs || !pidAlive(existing.pid);
      if (!stale) return null;
      // Steal only the exact lock we judged stale.
      try {
        if ((await fs.readFile(path, 'utf8')) === existingText) await fs.rm(path, { force: true });
      } catch {
        // someone else got there first
      }
    }
    return null;
  }

  private heldLock(path: string, info: LockInfo): StoreLock {
    let released = false;
    const beat = async () => {
      if (released) return;
      info.heartbeatAt = new Date().toISOString();
      try {
        const text = await fs.readFile(path, 'utf8');
        const cur = JSON.parse(text) as LockInfo;
        if (cur.owner !== info.owner) return; // stolen: do not overwrite
        await atomicWrite(path, JSON.stringify(info), this.root);
      } catch {
        // best effort
      }
    };
    const timer = setInterval(() => void beat(), this.heartbeatMs);
    return {
      info,
      release: async () => {
        if (released) return;
        released = true;
        clearInterval(timer);
        try {
          const cur = JSON.parse(await fs.readFile(path, 'utf8')) as LockInfo;
          if (cur.owner === info.owner) await fs.rm(path, { force: true });
        } catch {
          // gone
        }
      },
    };
  }
}
