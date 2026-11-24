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
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- test double for an invalid declaration
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

  test('rejects null, which is a wire encoding a backend must not use', () => {
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
  test('accepts a complete declaration', () => {
    const checked = wireOf(valid);

    expect(checked.ok).toBe(true);
    if (checked.ok) {
      expect(checked.capabilities).toEqual(valid);
    }
  });

  test('normalises a null ceiling to Infinity', () => {
    const checked = wireOf({ ...valid, maxTransactionDurationMs: null });

    expect(checked.ok).toBe(true);
    if (checked.ok) {
      expect(checked.capabilities.maxTransactionDurationMs).toBe(
        Number.POSITIVE_INFINITY,
      );
    }
  });

  test('accepts Infinity as unbounded', () => {
    const checked = wireOf({
      ...valid,
      maxTransactionDurationMs: Number.POSITIVE_INFINITY,
    });

    expect(checked.ok).toBe(true);
  });

  test('rejects a ceiling that would clamp the timeout', () => {
    for (const value of [0, -1, -0.5, Number.NaN, Number.NEGATIVE_INFINITY]) {
      const checked = wireOf({ ...valid, maxTransactionDurationMs: value });

      expect(checked.ok).toBe(false);
      expect(rejectionReasonOf(checked)).toContain('maxTransactionDurationMs');
    }
  });

  test('names the value path in the rejection reason', () => {
    const checked = wireOf({ ...valid, concurrency: 'multi' });

    expect(rejectionReasonOf(checked)).toContain('ready.capabilities');
  });
});

describe('the two entries agree', () => {
  /**
   * Regression guard: a value the backend self-check lets through but the wire
   * entry refuses would leave the Server ready and the Portal waiting for a
   * barrier that never arrives, with both sides logging normally.
   */
  test('the backend acceptance set is a subset of the wire acceptance set', () => {
    const ceilings: unknown[] = [
      Number.POSITIVE_INFINITY,
      1,
      0.5,
      5000,
      null,
      undefined,
      0,
      -0,
      -1,
      -0.5,
      Number.NaN,
      Number.NEGATIVE_INFINITY,
      '5000',
      true,
    ];
    const otherFields: Record<string, unknown>[] = [
      {},
      { transactions: 'limited', concurrency: 'single' },
      { transactions: 'partial' },
      { transactions: 7 },
      { concurrency: 'multi' },
      { snapshotRead: false, uniqueConstraint: false },
      { snapshotRead: 'true' },
      { uniqueConstraint: 1 },
    ];

    for (const ceiling of ceilings) {
      for (const extra of otherFields) {
        const candidate = {
          ...valid,
          ...extra,
          maxTransactionDurationMs: ceiling,
        };
        if (acceptsBackend(candidate) && !acceptsWire(candidate)) {
          throw new Error(
            `the backend check accepts what the wire entry rejects: ${JSON.stringify(candidate)}`,
          );
        }
      }
    }
  });

  test('only null tells the two acceptance sets apart', () => {
    const ceilings: unknown[] = [
      Number.POSITIVE_INFINITY,
      1,
      0.5,
      5000,
      null,
      undefined,
      0,
      -1,
      Number.NaN,
      Number.NEGATIVE_INFINITY,
      '5000',
    ];

    for (const ceiling of ceilings) {
      const candidate = { ...valid, maxTransactionDurationMs: ceiling };
      const backend = acceptsBackend(candidate);
      const wire = acceptsWire(candidate);

      if (ceiling === null) {
        expect(backend).toBe(false);
        expect(wire).toBe(true);
      } else {
        expect(wire).toBe(backend);
      }
    }
  });

  test('both entries normalise a null ceiling to the same value', () => {
    const wire = wireOf({ ...valid, maxTransactionDurationMs: null });
    const backend = backendOf({
      ...valid,
      maxTransactionDurationMs: Number.POSITIVE_INFINITY,
    });

    expect(wire.ok && backend.ok).toBe(true);
    if (wire.ok && backend.ok) {
      expect(wire.capabilities).toEqual(backend.capabilities);
    }
  });
});
