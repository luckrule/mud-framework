import { MAX_RESTART_BACKOFF_MS } from '../../shared/budget.ts';

export interface RestartPolicyOptions {
  /** Base for exponential restart backoff in ms. */
  baseBackoffMs: number;
  /**
   * How long a Server must stay up before its crash streak is considered over.
   *
   * The window must be at least as long as both the backoff cap and the
   * longest a single request can live: a Server that crashes while handling
   * its first request must be counted as a crash, not as a healthy run that
   * reset the backoff. Otherwise "ready, then die on the first request"
   * restarts at attempt 0 (immediate) forever.
   */
  stableResetMs: number;
}

/**
 * The crash-restart clock: the crash streak, the exponential backoff it drives
 * and the stability window that ends it. Both timers are owned here, so their
 * cleanup has one place.
 *
 * Split from the executor because none of it reads the process state machine:
 * whether a restart may be armed at all (`isTerminal`) and what a restart does
 * (`launch`) are the caller's decisions, so this class cannot restart a Server
 * on its own.
 */
export class RestartPolicy {
  private readonly baseBackoffMs: number;
  private readonly stableResetMs: number;
  /**
   * Crashes since the last stable run. Reset only by the stability window,
   * never by `ready`: a Server that reaches the handshake and then dies on its
   * first request is still part of the same crash streak.
   */
  private restartAttempts = 0;
  private restartTimer: ReturnType<typeof setTimeout> | undefined;
  private stableTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(options: RestartPolicyOptions) {
    this.baseBackoffMs = options.baseBackoffMs;
    this.stableResetMs = options.stableResetMs;
  }

  get attempts(): number {
    return this.restartAttempts;
  }

  /**
   * Exponential backoff, but the first restart of a crash streak is immediate:
   * a one-off crash should not cost the player 100ms of latency. The streak
   * only ends once a Server has been up for `stableResetMs`, so an immediate
   * restart cannot repeat indefinitely.
   */
  nextBackoffMs(): number {
    const attempt = this.restartAttempts;
    this.restartAttempts += 1;
    if (attempt === 0) {
      return 0;
    }
    return Math.min(
      this.baseBackoffMs * 2 ** (attempt - 1),
      MAX_RESTART_BACKOFF_MS,
    );
  }

  /**
   * Arms `launch` to run once after `backoffMs`. The field is cleared before
   * the callback runs, so a launch that fails can arm the next restart itself.
   */
  scheduleRestart(backoffMs: number, launch: () => void): void {
    this.restartTimer = setTimeout(() => {
      this.restartTimer = undefined;
      launch();
    }, backoffMs);
  }

  clearRestartTimer(): void {
    if (this.restartTimer === undefined) {
      return;
    }
    clearTimeout(this.restartTimer);
    this.restartTimer = undefined;
  }

  /**
   * Restarts the stability clock for the current Server. Only its expiry ends
   * the crash streak; `ready` alone does not, because a Server can reach the
   * handshake and still die on its first request.
   */
  startStableClock(): void {
    this.clearStableClock();
    this.stableTimer = setTimeout(() => {
      this.stableTimer = undefined;
      this.restartAttempts = 0;
    }, this.stableResetMs);
  }

  clearStableClock(): void {
    if (this.stableTimer === undefined) {
      return;
    }
    clearTimeout(this.stableTimer);
    this.stableTimer = undefined;
  }
}
