import { describe, expect, jest, test } from 'bun:test';

import { effectiveTimeoutMs, settleWithin, tightenTimeout } from './timeout.ts';

const TIGHTEN_CASES: readonly [string, number, number | undefined, number][] = [
  ['an undefined override keeps the ceiling', 1000, undefined, 1000],
  ['a NaN override is ignored', 1000, Number.NaN, 1000],
  ['a zero override is ignored', 1000, 0, 1000],
  ['a negative override is ignored', 1000, -500, 1000],
  // Infinity is not finite, so it takes the same unusable path as NaN: the
  // ceiling stands rather than an unbounded request being created.
  ['an infinite override is ignored', 1000, Number.POSITIVE_INFINITY, 1000],
  ['a smaller override wins', 1000, 250, 250],
  ['a larger override cannot relax the ceiling', 1000, 5000, 1000],
  ['an equal override returns the ceiling', 1000, 1000, 1000],
];

describe('tightenTimeout', () => {
  test.each(TIGHTEN_CASES)('%s', (_name, ceiling, override, expected) => {
    expect(tightenTimeout(ceiling, override)).toBe(expected);
  });
});

const EFFECTIVE_CASES: readonly [string, number, number, number][] = [
  ['two positive finite values yield the smaller one', 1000, 250, 250],
  ['the argument order does not matter', 250, 1000, 250],
  [
    'an infinite backend declaration falls back to the portal value',
    1000,
    Number.POSITIVE_INFINITY,
    1000,
  ],
  ['a zero backend declaration degrades to 1ms', 1000, 0, 1],
  ['a negative backend declaration degrades to 1ms', 1000, -1, 1],
  ['a NaN backend declaration degrades to 1ms', 1000, Number.NaN, 1],
  [
    'two very large values yield the smaller one',
    Number.MAX_SAFE_INTEGER,
    1e15,
    1e15,
  ],
];

describe('effectiveTimeoutMs', () => {
  test.each(EFFECTIVE_CASES)('%s', (_name, declared, ceiling, expected) => {
    expect(effectiveTimeoutMs(declared, ceiling)).toBe(expected);
  });
});

describe('settleWithin', () => {
  test('reports the value of a promise that settled first', async () => {
    const result = await settleWithin(Promise.resolve('done'), 1000);

    expect(result).toEqual({ status: 'settled', value: 'done' });
  });

  test('reports a rejection as rejected rather than throwing', async () => {
    const failure = new Error('boom');

    const result = await settleWithin(Promise.reject(failure), 1000);

    expect(result).toEqual({ status: 'rejected', error: failure });
  });

  test('reports a timeout when the promise outlives the budget', async () => {
    // Never settles: the only outcome is the timer.
    const result = await settleWithin(new Promise<never>(() => undefined), 10);

    expect(result).toEqual({ status: 'timeout' });
  });

  test('a rejection after the timeout is consumed, not left unhandled', async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);
    try {
      const late = Promise.withResolvers<void>();
      const result = await settleWithin(late.promise, 5);
      expect(result).toEqual({ status: 'timeout' });

      late.reject(new Error('rejected after the race was lost'));
      // The rejection surfaces on the next turn if nothing consumed it.
      await new Promise<void>((resolve) => {
        setTimeout(resolve, 20);
      });
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }

    expect(unhandled).toEqual([]);
  });

  test('clears its timer when the promise settles first', async () => {
    // A leftover timer would keep the event loop alive after shutdown finished.
    jest.useFakeTimers();
    try {
      const result = await settleWithin(Promise.resolve(1), 60_000);

      expect(result).toEqual({ status: 'settled', value: 1 });
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });
});
