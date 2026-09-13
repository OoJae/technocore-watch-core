import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWatcher, type Watcher, type WatcherOptions } from '../../src/engine/watcher.ts';
import { FileStateStore } from '../../src/state/file-store.ts';
import type { Notice } from '../../src/protocol/types.ts';
import type { WatchEvent } from '../../src/engine/types.ts';

export const SKIP_SLOW = process.env.TECHNOCORE_SKIP_SLOW === '1';

export async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs = 30_000, stepMs = 25): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await new Promise((r) => setTimeout(r, stepMs));
  }
  if (await cond()) return;
  throw new Error(`condition not met within ${timeoutMs}ms`);
}

export const sleepMs = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function tempDir(prefix = 'tcw data '): Promise<{ path: string; cleanup: () => Promise<void> }> {
  const path = await mkdtemp(join(tmpdir(), prefix));
  return { path, cleanup: () => rm(path, { recursive: true, force: true }) };
}

export interface Harnessed {
  watcher: Watcher;
  notices: Notice[];
  events: WatchEvent[];
}

export function harness(o: Omit<WatcherOptions, 'store'> & { store?: WatcherOptions['store']; dataDir?: string; mark?: boolean }): Harnessed {
  const store = o.store ?? new FileStateStore(o.dataDir!);
  const watcher = createWatcher({ ...o, store });
  const notices: Notice[] = [];
  const events: WatchEvent[] = [];
  watcher.onNotice(async (n) => {
    notices.push(n);
    if (o.mark !== false) await watcher.markNotified(n.id);
  });
  watcher.onEvent((e) => events.push(e));
  return { watcher, notices, events };
}

export async function drain(w: Watcher, room: string, max: number): Promise<number[][]> {
  const pages: number[][] = [];
  for (let i = 0; i < 10_000; i++) {
    const page = await w.readPage(room, { max });
    if (!page.messages.length && !page.gaps.length) break;
    pages.push(page.messages.map((m) => m.seq));
    await w.ack(room, page.toSeq);
    if (!page.hasMore) break;
  }
  return pages;
}
