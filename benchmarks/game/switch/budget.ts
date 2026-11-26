/**
 * Stop budgets shared by the benchmark Portal and the coordinator.
 *
 * ADR-0004: `ConnectionLayer.stop()` is an external stop bounded by
 * `C + max(C, E + S) + C` — connection-layer drain, executor close (the
 * executor's declared budget plus slack, or the layer's own timeout when the
 * executor declares none), then transport close. An external stop budget must
 * cover that complete bound, not just the executor's own budget.
 */

/** `ConnectionLayer.shutdownTimeoutMs`; also the executor's shutdown grace. */
export const PORTAL_SHUTDOWN_TIMEOUT_MS = 5000;

/**
 * `IpcExecutor.closeBudgetMs` for the 5000 ms shutdown grace:
 * `3 × shutdown + SHUTDOWN_ACK_SLACK_MS + 4 × EXIT_WAIT_TIMEOUT_MS` =
 * 15000 + 250 + 8000, from `packages/core/src/shared/budget.ts`.
 */
export const PORTAL_EXECUTOR_CLOSE_BUDGET_MS = 23_250;

/** `EXECUTOR_CLOSE_SLACK_MS` in `packages/core/src/portal/connection-layer.ts`. */
export const PORTAL_EXECUTOR_CLOSE_SLACK_MS = 1000;

/** Headroom above the legal bound, so a slow stop is not read as a hang. */
export const STOP_DIAGNOSTIC_MARGIN_MS = 1000;

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

/** 35250 ms for the current Portal assembly: the 34250 ms bound plus margin. */
export const PORTAL_STOP_BUDGET_MS = deriveExternalStopBudget({
  connectionShutdownMs: PORTAL_SHUTDOWN_TIMEOUT_MS,
  executorCloseBudgetMs: PORTAL_EXECUTOR_CLOSE_BUDGET_MS,
  executorCloseSlackMs: PORTAL_EXECUTOR_CLOSE_SLACK_MS,
});
