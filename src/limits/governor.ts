// Client-side token bucket mirroring the origin's per-IP read bucket, seeded from /config.
// A 429 penalizes the whole bucket (every room sharing this origin), so one refusal
// slows the pool instead of each poller rediscovering the wall on its own.
import { type Clock, sleep, systemClock } from './clock.ts';

export interface GovernorOptions {
  /** Requests per minute this client allows itself. */
  perMinute: number;
  /** Bucket capacity; defaults to max(2, ceil(perMinute / 4)). */
  burst?: number;
  clock?: Clock;
}

export class RateGovernor {
  private perMinute: number;
  private burst: number;
  private tokens: number;
  private last: number;
  private blockedUntil = 0;
  private readonly clock: Clock;
  granted = 0;
  penalties = 0;

  constructor(o: GovernorOptions) {
    this.clock = o.clock ?? systemClock;
    this.perMinute = Math.max(1e-6, o.perMinute);
    this.burst = o.burst ?? Math.max(2, Math.ceil(this.perMinute / 4));
    this.tokens = this.burst;
    this.last = this.clock.now();
  }

  get ratePerMinute(): number {
    return this.perMinute;
  }

  configure(perMinute: number, burst?: number): void {
    this.refill();
    this.perMinute = Math.max(1e-6, perMinute);
    this.burst = burst ?? Math.max(2, Math.ceil(this.perMinute / 4));
    this.tokens = Math.min(this.tokens, this.burst);
  }

  private refill(): void {
    const now = this.clock.now();
    const elapsed = Math.max(0, now - this.last);
    this.tokens = Math.min(this.burst, this.tokens + (elapsed * this.perMinute) / 60_000);
    this.last = now;
  }

  /** Milliseconds until a token could be taken (0 = now). */
  delayMs(): number {
    this.refill();
    const now = this.clock.now();
    const blocked = Math.max(0, this.blockedUntil - now);
    const need = this.tokens >= 1 ? 0 : Math.ceil(((1 - this.tokens) * 60_000) / this.perMinute);
    return Math.max(blocked, need);
  }

  tryTake(): boolean {
    if (this.delayMs() > 0) return false;
    this.tokens -= 1;
    this.granted++;
    return true;
  }

  async acquire(signal?: AbortSignal): Promise<void> {
    for (;;) {
      const d = this.delayMs();
      if (d <= 0) {
        this.tokens -= 1;
        this.granted++;
        return;
      }
      await sleep(d, signal, this.clock);
    }
  }

  /** Honour a 429: nothing leaves this origin before retryAfterS, and the bucket restarts empty. */
  penalize(retryAfterS: number): void {
    const until = this.clock.now() + Math.max(0, retryAfterS) * 1000;
    this.blockedUntil = Math.max(this.blockedUntil, until);
    this.refill();
    this.tokens = 0;
    this.last = Math.max(this.last, until);
    this.penalties++;
  }

  get blockedForMs(): number {
    return Math.max(0, this.blockedUntil - this.clock.now());
  }
}
