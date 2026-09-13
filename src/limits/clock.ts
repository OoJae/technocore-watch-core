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
export function sleep(ms: number, signal?: AbortSignal | readonly AbortSignal[], clock: Clock = systemClock): Promise<void> {
  // Several signals are watched directly rather than through AbortSignal.any, so a loop that
  // sleeps millions of times never builds up dependent signals on a long-lived parent.
  const signals: readonly AbortSignal[] = signal === undefined ? [] : Array.isArray(signal) ? signal : [signal as AbortSignal];
  return new Promise<void>((resolve, reject) => {
    if (signals.some((s) => s.aborted)) {
      reject(new AbortError());
      return;
    }
    const detach = () => {
      for (const s of signals) s.removeEventListener('abort', onAbort);
    };
    const onAbort = () => {
      clock.clearTimeout(handle);
      detach();
      reject(new AbortError());
    };
    const handle = clock.setTimeout(() => {
      detach();
      resolve();
    }, ms);
    for (const s of signals) s.addEventListener('abort', onAbort, { once: true });
  });
}
