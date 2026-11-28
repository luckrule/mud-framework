import { describe, expect, test } from 'bun:test';

import {
  closeBudget,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
} from '../../../packages/core/src/shared/budget.ts';
import { portalStopBudgetMs } from '../support.ts';
import {
  parsePortalCloseBudgetMs,
  resolveExecutorCloseBudget,
} from './budget.ts';
import { phaseLine, summarizePhase } from './support.ts';
import type { RequestSample, SwitchMarkers } from './support.ts';

describe('executor close budget from the Portal', () => {
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
      closeBudgetMs: closeBudget(DEFAULT_SHUTDOWN_TIMEOUT_MS),
      reported: false,
    });
  });
});

test('the external stop budget covers the ADR-0004 bound', () => {
  // C=5000 (Portal config), E=closeBudget(5000)=23250, S=1000, margin=1000.
  expect(portalStopBudgetMs()).toBe(35_250);
});

const MARKERS: SwitchMarkers = {
  measureStartNs: 0,
  endNs: 1_000_000,
  triggerNs: 100,
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
