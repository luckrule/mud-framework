import { describe, expect, test } from 'bun:test';

import {
  closeBudget,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
} from '../../../packages/core/src/shared/budget.ts';
import {
  PORTAL_EXECUTOR_CLOSE_BUDGET_MS,
  PORTAL_SHUTDOWN_TIMEOUT_MS,
  PORTAL_STOP_BUDGET_MS,
  STOP_DIAGNOSTIC_MARGIN_MS,
  deriveExternalStopBudget,
  parsePortalCloseBudgetMs,
  resolveExecutorCloseBudget,
} from './budget.ts';
import { phaseLine, summarizePhase } from './support.ts';
import type { RequestSample, SwitchMarkers } from './support.ts';

const PORTAL_BUDGET = {
  connectionShutdownMs: 5000,
  executorCloseBudgetMs: 23_250,
  executorCloseSlackMs: 1000,
} as const;

describe('external stop budget (ADR-0004)', () => {
  test('the benchmark Portal waits above the documented bound', () => {
    // C + max(C, E + S) + C = 5000 + 24250 + 5000 = 34250.
    expect(
      deriveExternalStopBudget({
        ...PORTAL_BUDGET,
        diagnosticMarginMs: 0,
      }),
    ).toBe(34_250);
    expect(PORTAL_STOP_BUDGET_MS).toBe(34_250 + STOP_DIAGNOSTIC_MARGIN_MS);
  });

  test('follows C + max(C, E + S) + C', () => {
    expect(
      deriveExternalStopBudget({
        connectionShutdownMs: 100,
        executorCloseBudgetMs: 1000,
        executorCloseSlackMs: 50,
        diagnosticMarginMs: 0,
      }),
    ).toBe(100 + 1050 + 100);
  });

  test('the connection timeout wins when E + S is smaller', () => {
    expect(
      deriveExternalStopBudget({
        connectionShutdownMs: 5000,
        executorCloseBudgetMs: 100,
        executorCloseSlackMs: 0,
        diagnosticMarginMs: 0,
      }),
    ).toBe(15_000);
  });

  test('an executor that declares no budget falls back to C', () => {
    expect(
      deriveExternalStopBudget({
        connectionShutdownMs: 5000,
        executorCloseBudgetMs: undefined,
        executorCloseSlackMs: 1000,
        diagnosticMarginMs: 0,
      }),
    ).toBe(15_000);
  });

  test('the diagnostic margin defaults and can be overridden', () => {
    const bound = deriveExternalStopBudget({
      ...PORTAL_BUDGET,
      diagnosticMarginMs: 0,
    });
    expect(deriveExternalStopBudget(PORTAL_BUDGET)).toBe(
      bound + STOP_DIAGNOSTIC_MARGIN_MS,
    );
    expect(
      deriveExternalStopBudget({ ...PORTAL_BUDGET, diagnosticMarginMs: 2500 }),
    ).toBe(bound + 2500);
  });
});

describe('executor close budget from the Portal (drift guard)', () => {
  test('the snapshot matches the core close budget', () => {
    // The Portal reports its live budget; this copy is the fallback, so it has
    // to be re-derived here. Asserting against the same constant would stay
    // green after core changed its formula.
    expect(PORTAL_EXECUTOR_CLOSE_BUDGET_MS).toBe(
      closeBudget(DEFAULT_SHUTDOWN_TIMEOUT_MS),
    );
    expect(PORTAL_SHUTDOWN_TIMEOUT_MS).toBe(DEFAULT_SHUTDOWN_TIMEOUT_MS);
  });

  test('reads the live budget the Portal prints', () => {
    expect(
      parsePortalCloseBudgetMs('portal: executor close budget 23250 ms'),
    ).toBe(23_250);
    // A changed core budget must win over the snapshot, whatever its value.
    expect(
      parsePortalCloseBudgetMs('portal: executor close budget 31000 ms'),
    ).toBe(31_000);
  });

  test('ignores every other stdout line', () => {
    const lines = [
      'portal: listening on ws://127.0.0.1:1234/',
      'portal: control on http://127.0.0.1:1235/switch',
      '{"level":"info","msg":"server ready","pid":42}',
      'portal: executor close budget 23250ms',
      'portal: executor close budget  23250 ms',
      'portal: executor close budget 0 ms',
      'portal: executor close budget 23250.5 ms',
      'portal: executor close budget abc ms',
      '',
    ];
    for (const line of lines) {
      expect(parsePortalCloseBudgetMs(line)).toBeUndefined();
    }
  });

  test('uses the reported budget and only falls back when none arrived', () => {
    expect(resolveExecutorCloseBudget(23_250)).toEqual({
      closeBudgetMs: 23_250,
      reported: true,
    });
    expect(resolveExecutorCloseBudget(31_000)).toEqual({
      closeBudgetMs: 31_000,
      reported: true,
    });
    expect(resolveExecutorCloseBudget(undefined)).toEqual({
      closeBudgetMs: PORTAL_EXECUTOR_CLOSE_BUDGET_MS,
      reported: false,
    });
  });

  test('a reported budget drives the stop budget, not the snapshot', () => {
    const reported = resolveExecutorCloseBudget(31_000);
    expect(
      deriveExternalStopBudget({
        connectionShutdownMs: 5000,
        executorCloseBudgetMs: reported.closeBudgetMs,
        executorCloseSlackMs: 1000,
        diagnosticMarginMs: 0,
      }),
    ).toBe(5000 + 32_000 + 5000);
  });
});

const MARKERS: SwitchMarkers = {
  measureStartNs: 0,
  endNs: 1_000_000,
  triggerNs: 100,
  resolvedNs: 200,
  windowEndNs: 300,
};

/** A sample that overlaps the switch window. */
function sample(code: string | undefined, ok = false): RequestSample {
  return { client: 0, startNs: 150, endNs: 250, ok, code };
}

describe('switch phase timeout accounting', () => {
  test('FRAMEWORK_TIMEOUT is a timeout, not an ordinary failure', () => {
    const summary = summarizePhase(
      [
        sample(undefined, true),
        sample('CLIENT_TIMEOUT'),
        sample('FRAMEWORK_TIMEOUT'),
        sample('ROOM_FULL'),
        sample('INTERNAL_ERROR'),
      ],
      MARKERS,
      'window',
    );

    expect(summary.requests).toBe(5);
    expect(summary.ok).toBe(1);
    expect(summary.clientTimeouts).toBe(1);
    expect(summary.frameworkTimeouts).toBe(1);
    expect(summary.timeouts).toBe(2);
    expect(summary.failed).toBe(2);
  });

  test('CLIENT_TIMEOUT accounting is unchanged', () => {
    const summary = summarizePhase(
      [sample('CLIENT_TIMEOUT')],
      MARKERS,
      'window',
    );

    expect(summary.timeouts).toBe(1);
    expect(summary.clientTimeouts).toBe(1);
    expect(summary.frameworkTimeouts).toBe(0);
    expect(summary.failed).toBe(0);
    expect(summary.disconnected).toBe(0);
  });

  test('an ordinary business error still counts as failed', () => {
    const summary = summarizePhase([sample('ROOM_FULL')], MARKERS, 'window');

    expect(summary.failed).toBe(1);
    expect(summary.timeouts).toBe(0);
    expect(summary.clientTimeouts).toBe(0);
    expect(summary.frameworkTimeouts).toBe(0);
  });

  test('the phase line shows the timeout breakdown', () => {
    const summary = summarizePhase(
      [sample('FRAMEWORK_TIMEOUT')],
      MARKERS,
      'window',
    );
    const line = phaseLine('window', summary);

    expect(line).toMatch(/timeout\s+1 \(client 0, framework 1\)/);
    expect(line).not.toMatch(/timeout\s+0\s/);
  });
});
