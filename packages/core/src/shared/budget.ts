/**
 * The time budgets the Portal spends on a Server, kept apart from the state
 * machine that spends them: the defaults, the constants the close formula is
 * built from, and the formula itself (ADR-0004).
 */

/** Max replays of one request before it reaches a terminal state. */
export const DEFAULT_MAX_REPLAYS = 3;

/** Base for exponential restart backoff in ms; the cap is `MAX_RESTART_BACKOFF_MS`. */
export const DEFAULT_RESTART_BACKOFF_MS = 100;

export const MAX_RESTART_BACKOFF_MS = 5000;

/** Grace period for a graceful shutdown before killing. */
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;

/**
 * Upper bound on how long a kill is awaited before the Portal moves on.
 * SIGKILL normally lands in milliseconds; a process stuck in uninterruptible
 * sleep would otherwise stall shutdown or restart forever.
 */
export const EXIT_WAIT_TIMEOUT_MS = 2000;

/**
 * Slack on top of the graceful-close grace before the Portal gives up on
 * `closed`. The grace is declared to the Server in the `shutdown` message, but
 * the Server starts spending it when the message arrives — one IPC hop after the
 * Portal started its own timer. Waiting exactly the declared grace would
 * therefore always end first and kill a Server that was about to send `closed`,
 * which is the defect the declaration exists to remove. The slack covers the
 * hop, so "the Server used its whole budget" is the observable outcome.
 */
export const SHUTDOWN_ACK_SLACK_MS = 250;

/**
 * Upper bound on requests buffered but not yet acknowledged. Their lifetime is
 * bounded by the request deadline, so this is not a steady-state limit; it is a
 * brake for the case the deadline cannot cover: a Server that stays unavailable
 * while a client keeps writing, which would otherwise grow the buffer until the
 * Portal runs out of memory.
 */
export const DEFAULT_MAX_PENDING = 10_000;

/**
 * The sequential waits a close performs, summed. See ADR-0004.
 *
 * 1. interrupt a `switchTo` in flight and wait for it to release its staged
 *    process — `shutdownTimeoutMs` (see `abortSwitchForClose`);
 * 2. drain the requests buffered but not yet acknowledged —
 *    `shutdownTimeoutMs`;
 * 3. send `shutdown` and wait for the Server's `closed` — the declared grace
 *    plus `SHUTDOWN_ACK_SLACK_MS` (see `waitForClosed`);
 * 4. wait for the process to exit — `EXIT_WAIT_TIMEOUT_MS`;
 * 5. if it did not, kill it and wait once more — `EXIT_WAIT_TIMEOUT_MS`;
 * 6. confirm the exit of a replacement a `close()`-aborted switch abandoned,
 *    which may already hold storage: wait → kill → wait once more —
 *    `2 * EXIT_WAIT_TIMEOUT_MS` (see `confirmAbandonedExits`).
 *
 * The two shutdown timeouts are not the same wait: one is Portal's own drain,
 * the other is the Server's graceful close, which is why both are counted.
 *
 * Step 3's grace is also the budget declared to the Server in the `shutdown`
 * message, so the Server's whole graceful close fits inside step 3 by
 * construction — that is what keeps a correctly closing Server from being
 * killed by step 5.
 */
export function closeBudget(shutdownTimeoutMs: number): number {
  // Step 1 and 2 share the same declared bound, so they are counted twice.
  const switchAbortMs = shutdownTimeoutMs;
  const drainMs = shutdownTimeoutMs;
  const gracefulCloseMs = shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS;
  const exitWaitMs = EXIT_WAIT_TIMEOUT_MS;
  const postKillExitWaitMs = EXIT_WAIT_TIMEOUT_MS;
  const abandonedExitWaitMs = EXIT_WAIT_TIMEOUT_MS * 2;
  return (
    switchAbortMs +
    drainMs +
    gracefulCloseMs +
    exitWaitMs +
    postKillExitWaitMs +
    abandonedExitWaitMs
  );
}
