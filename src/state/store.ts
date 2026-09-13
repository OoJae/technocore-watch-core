import type { Notice, WatchStateV1 } from '../protocol/types.ts';

export interface LockInfo {
  pid: number;
  startedAt: string;
  heartbeatAt: string;
  owner?: string;
  hostname?: string;
}

export interface StoreLock {
  readonly info: LockInfo;
  /** False once released, or once another process took the lock over. */
  isHeld?(): boolean;
  /** Called once if another process takes the lock over; the holder must stop polling. */
  onLost?(fn: () => void): () => void;
  release(): Promise<void>;
}

export interface StateStore {
  load(scopeKey: string): Promise<WatchStateV1 | null>;
  /**
   * Compare-and-set save. `expectedRevision` is the revision the caller last loaded
   * (0 = must not exist yet). Rejects with RevisionConflictError otherwise.
   * Resolves with the stored document (revision = expectedRevision + 1).
   */
  save(state: WatchStateV1, expectedRevision: number): Promise<WatchStateV1>;
  /** Cheap check of the stored revision (0 when absent). */
  revision(scopeKey: string): Promise<number>;
  appendNotice(scopeKey: string, notice: Notice): Promise<void>;
  readNotices(scopeKey: string): Promise<Notice[]>;
  listScopes(): Promise<string[]>;
  /** Single-poller lock. Resolves null when another live process holds it. */
  acquireLock(name?: string): Promise<StoreLock | null>;
  /** Where notices for a scope live, when the store is file-backed (for tailing). */
  noticesPath?(scopeKey: string): string;
}
