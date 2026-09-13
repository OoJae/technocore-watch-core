import type { Notice, WatchStateV1 } from '../protocol/types.ts';
import { MAX_STATE_BYTES, RevisionConflictError, StateError, cloneState } from './model.ts';
import type { LockInfo, StateStore, StoreLock } from './store.ts';

export class MemoryStateStore implements StateStore {
  private readonly states = new Map<string, WatchStateV1>();
  private readonly notices = new Map<string, Notice[]>();
  private readonly locks = new Set<string>();
  saves = 0;

  async load(scopeKey: string): Promise<WatchStateV1 | null> {
    const s = this.states.get(scopeKey);
    return s ? cloneState(s) : null;
  }

  async revision(scopeKey: string): Promise<number> {
    return this.states.get(scopeKey)?.revision ?? 0;
  }

  async save(state: WatchStateV1, expectedRevision: number): Promise<WatchStateV1> {
    const current = this.states.get(state.scopeKey)?.revision ?? 0;
    if (current !== expectedRevision) throw new RevisionConflictError(expectedRevision, current);
    const stored = { ...cloneState(state), revision: expectedRevision + 1, updatedAt: new Date().toISOString() };
    if (JSON.stringify(stored).length > MAX_STATE_BYTES) throw new StateError('state exceeds 256 KiB');
    this.states.set(state.scopeKey, stored);
    this.saves++;
    return cloneState(stored);
  }

  async appendNotice(scopeKey: string, notice: Notice): Promise<void> {
    const list = this.notices.get(scopeKey) ?? [];
    list.push(structuredClone(notice));
    if (list.length > 256) list.splice(0, list.length - 256);
    this.notices.set(scopeKey, list);
  }

  async readNotices(scopeKey: string): Promise<Notice[]> {
    return structuredClone(this.notices.get(scopeKey) ?? []);
  }

  async listScopes(): Promise<string[]> {
    return [...this.states.keys()];
  }

  async acquireLock(name = 'poller'): Promise<StoreLock | null> {
    if (this.locks.has(name)) return null;
    this.locks.add(name);
    const now = new Date().toISOString();
    const info: LockInfo = { pid: process.pid, startedAt: now, heartbeatAt: now };
    return {
      info,
      release: async () => {
        this.locks.delete(name);
      },
    };
  }
}
