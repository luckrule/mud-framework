import type { StorageCapabilities } from './types.ts';

export type DegradationCapability =
  | 'transactions'
  | 'concurrency'
  | 'snapshotRead'
  | 'uniqueConstraint';

export interface Degradation {
  /** The capability that is weaker than its strongest level (SPEC 4.2). */
  readonly capability: DegradationCapability;
  /** What the backend declares. */
  readonly declared: string | boolean;
  /** What the framework therefore does not provide. */
  readonly consequence: string;
}

/**
 * Lists the capabilities `declared` falls short of, one entry per dimension
 * (SPEC 4.3). Empty means the backend offers every capability at full strength,
 * so nothing is degraded and nothing needs warning about.
 */
export function degradations(declared: StorageCapabilities): Degradation[] {
  const found: Degradation[] = [];
  // Fixed order (transactions, concurrency, snapshotRead, uniqueConstraint) so
  // the startup warnings read the same way for every backend.
  switch (declared.transactions) {
    case 'full':
      break;
    case 'limited':
      found.push({
        capability: 'transactions',
        declared: declared.transactions,
        consequence:
          'cross-entity writes are not atomic; an Extension that needs multi-entity atomicity must declare the requirement',
      });
      break;
    case 'none':
      found.push({
        capability: 'transactions',
        declared: declared.transactions,
        consequence:
          'no rollback: after the first write fails, later writes in the same request are rejected by the framework (reads are unaffected) and writes already made are kept; the Extension must compensate or be idempotent',
      });
      break;
  }
  switch (declared.concurrency) {
    case 'pooled':
      break;
    case 'single':
      found.push({
        capability: 'concurrency',
        declared: declared.concurrency,
        consequence:
          'framework-level concurrency does not reach the storage layer; long transactions block other requests',
      });
      break;
  }
  if (!declared.snapshotRead) {
    found.push({
      capability: 'snapshotRead',
      declared: declared.snapshotRead,
      consequence:
        'repeated reads inside one transaction are not guaranteed to agree; the Extension must handle data changing under it',
    });
  }
  if (!declared.uniqueConstraint) {
    found.push({
      capability: 'uniqueConstraint',
      declared: declared.uniqueConstraint,
      consequence:
        'no atomic check-then-write; uniqueness must be guaranteed by the single writer or a business idempotency key',
    });
  }
  return found;
}
