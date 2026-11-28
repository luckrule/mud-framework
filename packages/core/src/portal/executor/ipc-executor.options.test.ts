import { describe, expect, test } from 'bun:test';

import {
  closeBudget,
  DEFAULT_MAX_RESTART_BACKOFF_MS,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
} from '../../shared/budget.ts';
import { MAX_TIMER_DELAY_MS } from '../../shared/validation.ts';
import type { IpcExecutorOptions } from './ipc-executor-options.ts';
import { resolveMaxRestartBackoffMs } from './ipc-executor-options.ts';
import { IpcExecutor } from './ipc-executor.ts';

function executorWith(options: Partial<IpcExecutorOptions> = {}): IpcExecutor {
  return new IpcExecutor({
    spawn: () => {
      throw new Error('spawn must not be called while validating options');
    },
    timeoutMs: 1000,
    ...options,
  });
}

const timerOptions = [
  [
    'shutdownTimeoutMs',
    (value: unknown) => executorWith({ shutdownTimeoutMs: value as number }),
  ],
  [
    'stableResetMs',
    (value: unknown) => executorWith({ stableResetMs: value as number }),
  ],
] as const;

describe.each(timerOptions)('IpcExecutor %s validation', (name, create) => {
  test.each([
    ['zero', 0, RangeError],
    ['negative', -1, RangeError],
    ['fractional', 1.5, RangeError],
    ['NaN', Number.NaN, RangeError],
    ['Infinity', Number.POSITIVE_INFINITY, RangeError],
    ['string', '20', TypeError],
    ['null', null, TypeError],
  ] as const)('rejects %s at construction', (_case, value, kind) => {
    expect(() => create(value)).toThrow(kind);
    expect(() => create(value)).toThrow(new RegExp(`${name} must be`));
  });

  test('accepts the smallest legal delay', () => {
    expect(() => create(1)).not.toThrow();
  });

  test('rejects a delay above the largest setTimeout delay', () => {
    expect(() => create(MAX_TIMER_DELAY_MS + 1)).toThrow(RangeError);
    expect(() => create(MAX_TIMER_DELAY_MS + 1)).toThrow(
      new RegExp(`${name} must not exceed`),
    );
  });

  test('accepts the largest setTimeout delay', () => {
    expect(() => create(MAX_TIMER_DELAY_MS)).not.toThrow();
  });
});

describe('IpcExecutor maxRestartBackoffMs validation', () => {
  test.each([
    ['zero', 0, RangeError],
    ['negative', -1, RangeError],
    ['fractional', 1.5, RangeError],
    ['NaN', Number.NaN, RangeError],
    ['Infinity', Number.POSITIVE_INFINITY, RangeError],
    ['string', '20', TypeError],
    ['null', null, TypeError],
  ] as const)('rejects %s at construction', (_case, value, kind) => {
    expect(() =>
      executorWith({ maxRestartBackoffMs: value as number }),
    ).toThrow(kind);
    expect(() =>
      executorWith({ maxRestartBackoffMs: value as number }),
    ).toThrow(/maxRestartBackoffMs must be/);
  });

  test('rejects a cap above the largest setTimeout delay', () => {
    expect(() =>
      executorWith({ maxRestartBackoffMs: MAX_TIMER_DELAY_MS + 1 }),
    ).toThrow(RangeError);
    expect(() =>
      executorWith({ maxRestartBackoffMs: Number.MAX_SAFE_INTEGER }),
    ).toThrow(RangeError);
    expect(() =>
      executorWith({ maxRestartBackoffMs: MAX_TIMER_DELAY_MS + 1 }),
    ).toThrow(/safe integer between 1 and .* \(2\^31-1\)/);
  });

  test('rejects a cap below the configured base', () => {
    expect(() =>
      executorWith({ restartBackoffMs: 100, maxRestartBackoffMs: 50 }),
    ).toThrow(RangeError);
    expect(() =>
      executorWith({ restartBackoffMs: 100, maxRestartBackoffMs: 50 }),
    ).toThrow(/maxRestartBackoffMs must be at least restartBackoffMs \(100\)/);
  });

  test('accepts a cap equal to the base and the smallest legal pair', () => {
    expect(() =>
      executorWith({ restartBackoffMs: 1, maxRestartBackoffMs: 1 }),
    ).not.toThrow();
  });

  test('accepts the largest setTimeout delay as a cap', () => {
    expect(() =>
      executorWith({ maxRestartBackoffMs: MAX_TIMER_DELAY_MS }),
    ).not.toThrow();
  });

  test('defaults to the historical 5000ms cap', () => {
    expect(resolveMaxRestartBackoffMs(undefined, 100)).toBe(
      DEFAULT_MAX_RESTART_BACKOFF_MS,
    );
    // An omitted cap must not reject a base above it: that configuration was
    // accepted before the option existed and stayed capped at the default.
    expect(resolveMaxRestartBackoffMs(undefined, 6000)).toBe(
      DEFAULT_MAX_RESTART_BACKOFF_MS,
    );
  });
});

describe('IpcExecutor timer option defaults', () => {
  test('keeps the documented defaults when neither option is set', () => {
    const executor = executorWith();
    expect(executor.closeBudgetMs).toBe(
      closeBudget(DEFAULT_SHUTDOWN_TIMEOUT_MS),
    );
  });
});
