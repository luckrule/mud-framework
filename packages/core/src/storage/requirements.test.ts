import { describe, expect, test } from 'bun:test';

import { unmetRequirements } from './requirements.ts';
import type { StorageRequirements } from './requirements.ts';
import type { StorageCapabilities } from './types.ts';

/** Every dimension at its weakest, so each test varies only what it names. */
const weakest: StorageCapabilities = {
  transactions: 'none',
  concurrency: 'single',
  snapshotRead: false,
  uniqueConstraint: false,
  maxTransactionDurationMs: Number.POSITIVE_INFINITY,
};

function backend(
  overrides: Partial<StorageCapabilities> = {},
): StorageCapabilities {
  return { ...weakest, ...overrides };
}

function unmet(
  capabilities: StorageCapabilities,
  requirements: StorageRequirements,
): string[] {
  return unmetRequirements(capabilities, requirements);
}

describe('unmetRequirements transactions', () => {
  const modes = ['full', 'limited', 'none'] as const;
  /** Weakest to strongest, independent of the label order above. */
  const strength = { none: 0, limited: 1, full: 2 } as const;

  for (const declared of modes) {
    for (const required of modes) {
      // A minimum, not an equality: asking for less than the backend offers is
      // satisfied, which is why the six combinations are not a matrix of equals.
      const satisfied = strength[declared] >= strength[required];
      test(`declares ${declared}, requires ${required}`, () => {
        const result = unmet(backend({ transactions: declared }), {
          transactions: required,
        });

        expect(result).toEqual(
          satisfied
            ? []
            : [`transactions: needs ${required}, backend declares ${declared}`],
        );
      });
    }
  }
});

describe('unmetRequirements concurrency', () => {
  const modes = ['single', 'pooled'] as const;
  /** Weakest to strongest: `pooled` is the stronger capability. */
  const strength = { single: 0, pooled: 1 } as const;

  for (const declared of modes) {
    for (const required of modes) {
      const satisfied = strength[declared] >= strength[required];
      test(`declares ${declared}, requires ${required}`, () => {
        const result = unmet(backend({ concurrency: declared }), {
          concurrency: required,
        });

        expect(result).toEqual(
          satisfied
            ? []
            : [`concurrency: needs ${required}, backend declares ${declared}`],
        );
      });
    }
  }

  test('requiring pooled on a single backend is unmet', () => {
    // The direction that is easy to invert: `single` is the safer execution
    // model (it serialises), but an Extension that needs parallel writes cannot
    // reach its throughput there, so the requirement is not met.
    expect(
      unmet(backend({ concurrency: 'single' }), { concurrency: 'pooled' }),
    ).toEqual(['concurrency: needs pooled, backend declares single']);
  });
});

describe('unmetRequirements boolean dimensions', () => {
  for (const declared of [true, false]) {
    test(`declares snapshotRead ${declared}, requires true`, () => {
      expect(
        unmet(backend({ snapshotRead: declared }), { snapshotRead: true }),
      ).toEqual(
        declared ? [] : ['snapshotRead: needs true, backend declares false'],
      );
    });

    test(`declares uniqueConstraint ${declared}, requires true`, () => {
      expect(
        unmet(backend({ uniqueConstraint: declared }), {
          uniqueConstraint: true,
        }),
      ).toEqual(
        declared
          ? []
          : ['uniqueConstraint: needs true, backend declares false'],
      );
    });
  }

  test('requiring false is no constraint', () => {
    // `false` means "I do not depend on it", not "it must be absent": a backend
    // that offers the capability is always usable.
    expect(
      unmet(backend({ snapshotRead: true, uniqueConstraint: true }), {
        snapshotRead: false,
        uniqueConstraint: false,
      }),
    ).toEqual([]);
  });
});

describe('unmetRequirements overall', () => {
  test('empty requirements are always satisfied', () => {
    expect(unmet(weakest, {})).toEqual([]);
    expect(
      unmet(
        backend({
          transactions: 'full',
          concurrency: 'pooled',
          snapshotRead: true,
          uniqueConstraint: true,
        }),
        {},
      ),
    ).toEqual([]);
  });

  test('reports every unmet dimension, not just the first', () => {
    const result = unmet(weakest, {
      transactions: 'full',
      concurrency: 'pooled',
      snapshotRead: true,
      uniqueConstraint: true,
    });

    expect(result).toEqual([
      'transactions: needs full, backend declares none',
      'concurrency: needs pooled, backend declares single',
      'snapshotRead: needs true, backend declares false',
      'uniqueConstraint: needs true, backend declares false',
    ]);
  });

  test('a satisfied requirement contributes no phrase', () => {
    const result = unmet(backend({ transactions: 'full' }), {
      transactions: 'full',
      concurrency: 'pooled',
    });

    expect(result).toEqual([
      'concurrency: needs pooled, backend declares single',
    ]);
  });
});
