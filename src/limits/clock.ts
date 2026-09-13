// Time source and abortable sleeps. Resolved at call time so fake timers can intercept.

export type TimerHandle = unknown;

export interface Clock {
  /** Epoch milliseconds. */
  now(): number;
  setTimeout(fn: () => void, ms: number): TimerHandle;
  clearTimeout(handle: TimerHandle): void;
}

export const systemClock: Clock = {
  now: () => Date.now(),
  setTimeout: (fn, ms) => globalThis.setTimeout(fn, Math.max(0, ms)),
  clearTimeout: (h) => globalThis.clearTimeout(h as ReturnType<typeof globalThis.setTimeout>),
};

export class AbortError extends Error {
  override readonly name = 'AbortError';
  constructor(message = 'aborted') {
    super(message);
  }
}

export function isAbortError(e: unknown): boolean {
  return e instanceof Error && (e.name === 'AbortError' || (e as { code?: string }).code === 'ABORT_ERR');
}

export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw new AbortError();
}

/** Sleep `ms`, rejecting with AbortError as soon as `signal` aborts. Abort beats every sleep. */
export function sleep(ms: number, signal?: AbortSignal, clock: Clock = systemClock): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new AbortError());
      return;
    }
    const onAbort = () => {
      clock.clearTimeout(handle);
      reject(new AbortError());
    };
    const handle = clock.setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
