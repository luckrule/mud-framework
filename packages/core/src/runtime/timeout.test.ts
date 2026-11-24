import { describe, expect, test } from 'bun:test';

import { effectiveTimeoutMs, tightenTimeout } from './timeout.ts';

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
