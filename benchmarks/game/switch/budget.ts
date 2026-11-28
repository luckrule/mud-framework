import {
  closeBudget,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
} from '../../../packages/core/src/shared/budget.ts';

/**
 * The executor close budget the switch Portal reports, and its fallback.
 *
 * `E` is read from the running Portal rather than assumed: the Portal prints
 * the `IpcExecutor.closeBudgetMs` core computes, so a changed core budget
 * cannot leave the benchmark reporting a stale copy. The fallback is derived
 * from core's own close formula for the default shutdown grace.
 */

/** The Portal's stdout line reporting its live executor close budget. */
const CLOSE_BUDGET_LINE = /^portal: executor close budget (\d+) ms$/;

/**
 * Reads `IpcExecutor.closeBudgetMs` from the Portal's own stdout. `undefined`
 * for any other line, including a malformed one: the caller falls back rather
 * than reporting a value it could not read.
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
  /** The budget to report. */
  readonly closeBudgetMs: number;
  /** False when the Portal never reported one and the core fallback was used. */
  readonly reported: boolean;
}

/**
 * Prefers the budget the running Portal reported; the fallback is derived from
 * core, so it cannot drift from `closeBudget`.
 */
export function resolveExecutorCloseBudget(
  reportedMs: number | undefined,
): ResolvedExecutorCloseBudget {
  return reportedMs === undefined
    ? {
        closeBudgetMs: closeBudget(DEFAULT_SHUTDOWN_TIMEOUT_MS),
        reported: false,
      }
    : { closeBudgetMs: reportedMs, reported: true };
}
