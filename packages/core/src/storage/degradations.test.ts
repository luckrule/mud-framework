import { describe, expect, test } from 'bun:test';

import { degradations } from './degradations.ts';
import type { StorageCapabilities } from './types.ts';

/** Every dimension at full strength, so nothing is degraded. */
const strongest: StorageCapabilities = {
  transactions: 'full',
  concurrency: 'pooled',
  snapshotRead: true,
  uniqueConstraint: true,
  maxTransactionDurationMs: 5000,
};

function capabilitiesOf(
  overrides: Partial<StorageCapabilities>,
): StorageCapabilities {
  return { ...strongest, ...overrides };
}

describe('degradations', () => {
  test('a full declaration has nothing to warn about', () => {
    expect(degradations(strongest)).toEqual([]);
  });

  test('transactions: limited is one degradation', () => {
    const found = degradations(capabilitiesOf({ transactions: 'limited' }));

    expect(found).toHaveLength(1);
    expect(found[0]).toMatchObject({
      capability: 'transactions',
      declared: 'limited',
    });
  });

  test('transactions: none is one degradation', () => {
    const found = degradations(capabilitiesOf({ transactions: 'none' }));

    expect(found).toHaveLength(1);
    expect(found[0]).toMatchObject({
      capability: 'transactions',
      declared: 'none',
    });
  });

  test('concurrency: single is one degradation', () => {
    const found = degradations(capabilitiesOf({ concurrency: 'single' }));

    expect(found).toHaveLength(1);
    expect(found[0]).toMatchObject({
      capability: 'concurrency',
      declared: 'single',
    });
  });

  test('snapshotRead: false is one degradation', () => {
    const found = degradations(capabilitiesOf({ snapshotRead: false }));

    expect(found).toHaveLength(1);
    expect(found[0]).toMatchObject({
      capability: 'snapshotRead',
      declared: false,
    });
  });

  test('uniqueConstraint: false is one degradation', () => {
    const found = degradations(capabilitiesOf({ uniqueConstraint: false }));

    expect(found).toHaveLength(1);
    expect(found[0]).toMatchObject({
      capability: 'uniqueConstraint',
      declared: false,
    });
  });

  test('a fully weak declaration lists all four in a fixed order', () => {
    const found = degradations({
      transactions: 'none',
      concurrency: 'single',
      snapshotRead: false,
      uniqueConstraint: false,
      maxTransactionDurationMs: Number.POSITIVE_INFINITY,
    });

    expect(found.map((degradation) => degradation.capability)).toEqual([
      'transactions',
      'concurrency',
      'snapshotRead',
      'uniqueConstraint',
    ]);
  });

  test('maxTransactionDurationMs is not a degradation dimension', () => {
    // It is a ceiling, not a capability: any positive value is fully supported.
    expect(
      degradations(capabilitiesOf({ maxTransactionDurationMs: 1 })),
    ).toEqual([]);
  });

  test('every degradation explains what the framework does not provide', () => {
    const found = degradations({
      transactions: 'none',
      concurrency: 'single',
      snapshotRead: false,
      uniqueConstraint: false,
      maxTransactionDurationMs: Number.POSITIVE_INFINITY,
    });

    expect(found).toHaveLength(4);
    for (const degradation of found) {
      expect(degradation.consequence.length).toBeGreaterThan(0);
    }
  });
});
