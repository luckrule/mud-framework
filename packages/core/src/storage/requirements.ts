import type { StorageCapabilities } from './types.ts';

/**
 * What an Extension cannot work without (SPEC 4.3). Each field is a minimum,
 * not a full description: an Extension that omits a dimension runs anywhere,
 * and one that names a level refuses to run below it.
 */
export interface StorageRequirements {
  /** Minimum transaction strength: 'none' < 'limited' < 'full'. */
  transactions?: 'full' | 'limited' | 'none';
  /** Minimum concurrency: 'single' < 'pooled'. */
  concurrency?: 'single' | 'pooled';
  /** Requires snapshot-consistent reads inside a transaction. */
  snapshotRead?: boolean;
  /** Requires atomic check-then-write. */
  uniqueConstraint?: boolean;
}

/** Weakest to strongest; compared numerically rather than as labels. */
const TRANSACTION_STRENGTH: Record<
  StorageCapabilities['transactions'],
  number
> = {
  none: 0,
  limited: 1,
  full: 2,
};

/**
 * Also weakest to strongest as a *capability*: `pooled` is the stronger one,
 * even though `single` is the safer execution model (SPEC 4.4). An Extension
 * that needs parallel writes cannot reach its throughput on a backend that
 * serialises, so requiring `pooled` on a `single` backend is unmet. Inverting
 * this comparison would let that downgrade pass silently.
 */
const CONCURRENCY_STRENGTH: Record<StorageCapabilities['concurrency'], number> =
  {
    single: 0,
    pooled: 1,
  };

/**
 * Returns the requirements `capabilities` does not meet, as human-readable
 * phrases for a log line. Empty means satisfied.
 */
export function unmetRequirements(
  capabilities: StorageCapabilities,
  requirements: StorageRequirements,
): string[] {
  const unmet: string[] = [];
  const transactions = requirements.transactions;
  if (
    transactions !== undefined &&
    TRANSACTION_STRENGTH[capabilities.transactions] <
      TRANSACTION_STRENGTH[transactions]
  ) {
    unmet.push(
      `transactions: needs ${transactions}, backend declares ${capabilities.transactions}`,
    );
  }
  const concurrency = requirements.concurrency;
  if (
    concurrency !== undefined &&
    CONCURRENCY_STRENGTH[capabilities.concurrency] <
      CONCURRENCY_STRENGTH[concurrency]
  ) {
    unmet.push(
      `concurrency: needs ${concurrency}, backend declares ${capabilities.concurrency}`,
    );
  }
  if (requirements.snapshotRead === true && !capabilities.snapshotRead) {
    unmet.push('snapshotRead: needs true, backend declares false');
  }
  if (
    requirements.uniqueConstraint === true &&
    !capabilities.uniqueConstraint
  ) {
    unmet.push('uniqueConstraint: needs true, backend declares false');
  }
  return unmet;
}
