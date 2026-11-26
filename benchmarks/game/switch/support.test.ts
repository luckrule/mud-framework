import { describe, expect, test } from 'bun:test';

import {
  PORTAL_STOP_BUDGET_MS,
  STOP_DIAGNOSTIC_MARGIN_MS,
  deriveExternalStopBudget,
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
