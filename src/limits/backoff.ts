// Decorrelated jitter ("Exponential Backoff and Jitter", AWS Architecture Blog):
//   sleep = min(cap, random_between(base, previous * 3)), never below floor.

export interface JitterOptions {
  baseMs: number;
  capMs: number;
  floorMs?: number;
  random?: () => number;
}

export class DecorrelatedJitter {
  private prev: number;
  private readonly base: number;
  private readonly cap: number;
  private readonly floor: number;
  private readonly random: () => number;

  constructor(o: JitterOptions) {
    this.base = Math.max(1, o.baseMs);
    this.cap = Math.max(this.base, o.capMs);
    this.floor = Math.max(0, o.floorMs ?? 0);
    this.random = o.random ?? Math.random;
    this.prev = this.base;
  }

  next(): number {
    const hi = Math.max(this.base, this.prev * 3);
    const v = Math.min(this.cap, this.base + this.random() * (hi - this.base));
    this.prev = v;
    return Math.max(this.floor, Math.round(v));
  }

  reset(): void {
    this.prev = this.base;
  }
}

export function decorrelatedJitter(o: JitterOptions): DecorrelatedJitter {
  return new DecorrelatedJitter(o);
}

/** `ms` scaled by a uniform factor in [1 - spread, 1 + spread]. */
export function jitter(ms: number, spread = 0.2, random: () => number = Math.random): number {
  return Math.max(0, Math.round(ms * (1 - spread + 2 * spread * random())));
}
