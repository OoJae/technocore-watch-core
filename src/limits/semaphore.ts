// Counting semaphore with FIFO waiters and abort support.
import { AbortError } from './clock.ts';

interface Waiter {
  resolve: (release: () => void) => void;
  reject: (e: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

export class WaiterSemaphore {
  private inUse = 0;
  private peak = 0;
  private readonly queue: Waiter[] = [];
  private capacity: number;

  constructor(capacity: number) {
    if (!(capacity >= 1)) throw new RangeError('semaphore capacity must be >= 1');
    this.capacity = Math.floor(capacity);
  }

  get available(): number {
    return Math.max(0, this.capacity - this.inUse);
  }

  get active(): number {
    return this.inUse;
  }

  get maxObserved(): number {
    return this.peak;
  }

  get waiting(): number {
    return this.queue.length;
  }

  setCapacity(capacity: number): void {
    this.capacity = Math.max(1, Math.floor(capacity));
    this.drain();
  }

  tryAcquire(): (() => void) | null {
    if (this.inUse >= this.capacity || this.queue.length) return null;
    return this.grant();
  }

  acquire(signal?: AbortSignal): Promise<() => void> {
    if (signal?.aborted) return Promise.reject(new AbortError());
    const immediate = this.tryAcquire();
    if (immediate) return Promise.resolve(immediate);
    return new Promise((resolve, reject) => {
      const w: Waiter = { resolve, reject };
      if (signal) {
        w.signal = signal;
        w.onAbort = () => {
          const i = this.queue.indexOf(w);
          if (i >= 0) this.queue.splice(i, 1);
          reject(new AbortError());
        };
        signal.addEventListener('abort', w.onAbort, { once: true });
      }
      this.queue.push(w);
    });
  }

  async with<T>(fn: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const release = await this.acquire(signal);
    try {
      return await fn();
    } finally {
      release();
    }
  }

  private grant(): () => void {
    this.inUse++;
    this.peak = Math.max(this.peak, this.inUse);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.inUse--;
      this.drain();
    };
  }

  private drain(): void {
    while (this.inUse < this.capacity && this.queue.length) {
      const w = this.queue.shift()!;
      if (w.signal && w.onAbort) w.signal.removeEventListener('abort', w.onAbort);
      w.resolve(this.grant());
    }
  }
}
