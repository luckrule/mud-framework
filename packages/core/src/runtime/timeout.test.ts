import { describe, expect, jest, test } from 'bun:test';

import { effectiveTimeoutMs, settleWithin, tightenTimeout } from './timeout.ts';

describe('tightenTimeout', () => {
  test('an undefined override keeps the ceiling', () => {
    expect(tightenTimeout(1000, undefined)).toBe(1000);
  });

  test('a NaN override is ignored', () => {
    expect(tightenTimeout(1000, Number.NaN)).toBe(1000);
  });

  test('a zero override is ignored', () => {
    expect(tightenTimeout(1000, 0)).toBe(1000);
  });

  test('a negative override is ignored', () => {
    expect(tightenTimeout(1000, -500)).toBe(1000);
  });

  // Infinity is not finite, so it takes the same unusable path as NaN: the
  // ceiling stands rather than an unbounded request being created.
  test('an infinite override is ignored', () => {
    expect(tightenTimeout(1000, Number.POSITIVE_INFINITY)).toBe(1000);
  });

  test('a smaller override wins', () => {
    expect(tightenTimeout(1000, 250)).toBe(250);
  });

  test('a larger override cannot relax the ceiling', () => {
    expect(tightenTimeout(1000, 5000)).toBe(1000);
  });

  test('an equal override returns the ceiling', () => {
    expect(tightenTimeout(1000, 1000)).toBe(1000);
  });
});

describe('effectiveTimeoutMs', () => {
  test('two positive finite values yield the smaller one', () => {
    expect(effectiveTimeoutMs(1000, 250)).toBe(250);
    expect(effectiveTimeoutMs(250, 1000)).toBe(250);
  });

  test('an infinite backend declaration falls back to the portal value', () => {
    expect(effectiveTimeoutMs(1000, Number.POSITIVE_INFINITY)).toBe(1000);
  });

  test('a zero backend declaration degrades to 1ms', () => {
    expect(effectiveTimeoutMs(1000, 0)).toBe(1);
  });

  test('a negative backend declaration degrades to 1ms', () => {
    expect(effectiveTimeoutMs(1000, -1)).toBe(1);
  });

  test('a NaN backend declaration degrades to 1ms', () => {
    expect(effectiveTimeoutMs(1000, Number.NaN)).toBe(1);
  });

  test('two very large values yield the smaller one', () => {
    expect(effectiveTimeoutMs(Number.MAX_SAFE_INTEGER, 1e15)).toBe(1e15);
    expect(effectiveTimeoutMs(1e15, Number.MAX_SAFE_INTEGER)).toBe(1e15);
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
