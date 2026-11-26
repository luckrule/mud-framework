/**
 * Stop budgets shared by the benchmark Portal and the coordinator.
 *
 * ADR-0004: `ConnectionLayer.stop()` is an external stop bounded by
 * `C + max(C, E + S) + C` — connection-layer drain, executor close (the
 * executor's declared budget plus slack, or the layer's own timeout when the
 * executor declares none), then transport close. An external stop budget must
 * cover that complete bound, not just the executor's own budget.
 *
 * `E` is read from the running Portal rather than assumed: the Portal prints
 * the `IpcExecutor.closeBudgetMs` core computes, so a changed core budget
 * cannot leave the benchmark waiting on a stale copy.
 */

/** `ConnectionLayer.shutdownTimeoutMs`; also the executor's shutdown grace. */
export const PORTAL_SHUTDOWN_TIMEOUT_MS = 5000;

/**
 * Fallback `IpcExecutor.closeBudgetMs` for the 5000 ms shutdown grace:
 * `3 × shutdown + SHUTDOWN_ACK_SLACK_MS + 4 × EXIT_WAIT_TIMEOUT_MS` =
 * 15000 + 250 + 8000, from `packages/core/src/shared/budget.ts`. The Portal
 * reports the live value; this copy is only used when that line cannot be read.
 */
export const PORTAL_EXECUTOR_CLOSE_BUDGET_MS = 23_250;

/** `EXECUTOR_CLOSE_SLACK_MS` in `packages/core/src/portal/connection-layer.ts`. */
export const PORTAL_EXECUTOR_CLOSE_SLACK_MS = 1000;

/** Headroom above the legal bound, so a slow stop is not read as a hang. */
export const STOP_DIAGNOSTIC_MARGIN_MS = 1000;

/** The Portal's stdout line reporting its live executor close budget. */
const CLOSE_BUDGET_LINE = /^portal: executor close budget (\d+) ms$/;

/**
 * Reads `IpcExecutor.closeBudgetMs` from the Portal's own stdout. `undefined`
 * for any other line, including a malformed one: the caller falls back rather
 * than deriving a stop budget from a value it could not read.
 */
export function parsePortalCloseBudgetMs(line: string): number | undefined {
  const digits = CLOSE_BUDGET_LINE.exec(line)?.[1];
  if (digits === undefined) {
    return undefined;
  }
  const value = Number(digits);
  return Number.isSafeInteger(value) && value > 0 ? value : undefined;
}

export interface ResolvedExecutorCloseBudget {
  /** What the stop budget is derived from. */
  readonly closeBudgetMs: number;
  /** False when the Portal never reported one and the snapshot was used. */
  readonly reported: boolean;
}

/**
 * Prefers the budget the running Portal reported; the constant is only a
 * fallback, so a changed core budget cannot leave the benchmark waiting on a
 * stale copy of it.
 */
export function resolveExecutorCloseBudget(
  reportedMs: number | undefined,
): ResolvedExecutorCloseBudget {
  return reportedMs === undefined
    ? { closeBudgetMs: PORTAL_EXECUTOR_CLOSE_BUDGET_MS, reported: false }
    : { closeBudgetMs: reportedMs, reported: true };
}

export interface ExternalStopBudgetInput {
  /** C: `ConnectionLayer.shutdownTimeoutMs`. */
  readonly connectionShutdownMs: number;
  /**
   * E: `RequestExecutor.closeBudgetMs`. A local executor declares none, and the
   * layer then uses its own timeout for that step.
   */
  readonly executorCloseBudgetMs: number | undefined;
  /** S: the slack `ConnectionLayer.stop()` adds to E. */
  readonly executorCloseSlackMs: number;
  /** Extra wait for diagnostics; defaults to `STOP_DIAGNOSTIC_MARGIN_MS`. */
  readonly diagnosticMarginMs?: number;
}

export function deriveExternalStopBudget(
  input: ExternalStopBudgetInput,
): number {
  const connectionMs = input.connectionShutdownMs;
  const executorMs =
    input.executorCloseBudgetMs === undefined
      ? connectionMs
      : Math.max(
          connectionMs,
          input.executorCloseBudgetMs + input.executorCloseSlackMs,
        );
  return (
    connectionMs +
    executorMs +
    connectionMs +
    (input.diagnosticMarginMs ?? STOP_DIAGNOSTIC_MARGIN_MS)
  );
}

/**
 * The stop budget derived from the snapshot constant: the value used when the
 * Portal's reported budget could not be read, and the bound the unit tests
 * check against the documented derivation.
 */
export const PORTAL_STOP_BUDGET_MS = deriveExternalStopBudget({
  connectionShutdownMs: PORTAL_SHUTDOWN_TIMEOUT_MS,
  executorCloseBudgetMs: PORTAL_EXECUTOR_CLOSE_BUDGET_MS,
  executorCloseSlackMs: PORTAL_EXECUTOR_CLOSE_SLACK_MS,
});
