import { describe, expect, test } from 'bun:test';

import {
  checkBackendCapabilities,
  checkWireCapabilities,
} from './capabilities.ts';
import type { StorageCapabilities } from './types.ts';

const valid: StorageCapabilities = {
  transactions: 'full',
  concurrency: 'pooled',
  snapshotRead: true,
  uniqueConstraint: true,
  maxTransactionDurationMs: 5000,
};

/**
 * A backend that lies about its declaration is exactly what the self-check
 * exists to catch, so the test has to hand it a value the type system rejects.
 */
function backendOf(
  declared: unknown,
): ReturnType<typeof checkBackendCapabilities> {
  return checkBackendCapabilities(declared as StorageCapabilities);
}

function wireOf(raw: unknown): ReturnType<typeof checkWireCapabilities> {
  return checkWireCapabilities(raw, 'ready.capabilities');
}

function acceptsBackend(raw: unknown): boolean {
  return backendOf(raw).ok;
}

function acceptsWire(raw: unknown): boolean {
  return wireOf(raw).ok;
}

function rejectionReasonOf(
  result: ReturnType<typeof checkBackendCapabilities>,
): string {
  if (result.ok) {
    throw new Error('expected the declaration to be rejected');
  }
  return result.reason;
}

describe('checkBackendCapabilities', () => {
  test('accepts a complete declaration', () => {
    const checked = backendOf(valid);

    expect(checked.ok).toBe(true);
    if (checked.ok) {
      expect(checked.capabilities).toEqual(valid);
    }
  });

  test('accepts Infinity as unbounded', () => {
    const checked = backendOf({
      ...valid,
      maxTransactionDurationMs: Number.POSITIVE_INFINITY,
    });

    expect(checked.ok).toBe(true);
    if (checked.ok) {
      expect(checked.capabilities.maxTransactionDurationMs).toBe(
        Number.POSITIVE_INFINITY,
      );
    }
  });

  test('rejects null, which is not a duration', () => {
    const checked = backendOf({ ...valid, maxTransactionDurationMs: null });

    expect(checked.ok).toBe(false);
    expect(rejectionReasonOf(checked)).toContain('maxTransactionDurationMs');
  });

  test('rejects a ceiling that would clamp the timeout', () => {
    for (const value of [0, -1, -0.5, Number.NaN, Number.NEGATIVE_INFINITY]) {
      const checked = backendOf({ ...valid, maxTransactionDurationMs: value });

      expect(checked.ok).toBe(false);
      expect(rejectionReasonOf(checked)).toContain('maxTransactionDurationMs');
    }
  });

  test('rejects a capability value outside its enum', () => {
    const checked = backendOf({ ...valid, transactions: 'partial' });

    expect(checked.ok).toBe(false);
    expect(rejectionReasonOf(checked)).toContain('transactions');
  });

  test('rejects a capability that is not a boolean', () => {
    const checked = backendOf({ ...valid, snapshotRead: 'yes' });

    expect(checked.ok).toBe(false);
    expect(rejectionReasonOf(checked)).toContain('snapshotRead');
  });

  test('rejects a value that is not an object', () => {
    const checked = backendOf('capabilities');

    expect(checked.ok).toBe(false);
    expect(rejectionReasonOf(checked)).toContain('capabilities');
  });
});

describe('checkWireCapabilities', () => {
  test('names the value path in the rejection reason', () => {
    const checked = wireOf({ ...valid, concurrency: 'multi' });

    expect(rejectionReasonOf(checked)).toContain('ready.capabilities');
  });

  test('rejects a value that is not an object', () => {
    const checked = wireOf('capabilities');

    expect(checked.ok).toBe(false);
    expect(rejectionReasonOf(checked)).toContain('ready.capabilities');
  });

  test('rejects a null ceiling shipped over the wire', () => {
    const checked = wireOf({ ...valid, maxTransactionDurationMs: null });

    expect(checked.ok).toBe(false);
    expect(rejectionReasonOf(checked)).toContain('maxTransactionDurationMs');
  });

  test('accepts a complete declaration and returns it', () => {
    const checked = wireOf(valid);

    expect(checked.ok).toBe(true);
    if (checked.ok) {
      expect(checked.capabilities).toEqual(valid);
    }
  });
});

describe('the two entries agree', () => {
  /**
   * Regression guard: a value the backend self-check lets through but the wire
   * entry refuses would leave the Server ready and the Portal waiting for a
   * barrier that never arrives, with both sides logging normally.
   */
  test('both entries reach the expected verdict on every candidate', () => {
    // The expected verdict is written out per candidate rather than compared
    // between the two entries: both call `validate`, so "the two agree" is true
    // by construction and would assert nothing.
    const ceilings: readonly [unknown, boolean][] = [
      [Number.POSITIVE_INFINITY, true],
      [1, true],
      [0.5, true],
      [5000, true],
      [null, false],
      [undefined, false],
      [0, false],
      [-0, false],
      [-1, false],
      [-0.5, false],
      [Number.NaN, false],
      [Number.NEGATIVE_INFINITY, false],
      ['5000', false],
      [true, false],
    ];
    const otherFields: readonly [Record<string, unknown>, boolean][] = [
      [{}, true],
      [{ transactions: 'limited', concurrency: 'single' }, true],
      [{ snapshotRead: false, uniqueConstraint: false }, true],
      [{ transactions: 'partial' }, false],
      [{ transactions: 7 }, false],
      [{ concurrency: 'multi' }, false],
      [{ snapshotRead: 'true' }, false],
      [{ uniqueConstraint: 1 }, false],
    ];

    for (const [ceiling, ceilingAccepted] of ceilings) {
      for (const [extra, extraAccepted] of otherFields) {
        const candidate = {
          ...valid,
          ...extra,
          maxTransactionDurationMs: ceiling,
        };
        const expected = ceilingAccepted && extraAccepted;
        expect(acceptsWire(candidate)).toBe(expected);
        expect(acceptsBackend(candidate)).toBe(expected);
      }
    }
  });
});
