export interface StorageCapabilities {
  transactions: 'full' | 'limited' | 'none';
  concurrency: 'single' | 'pooled';
  snapshotRead: boolean;
  uniqueConstraint: boolean;
  /** Backend's own ceiling; declare Number.POSITIVE_INFINITY when unbounded. */
  maxTransactionDurationMs: number;
}

export interface StorageTransaction {
  query<T>(sql: string, params?: readonly unknown[]): Promise<T[]>;
  execute(sql: string, params?: readonly unknown[]): Promise<void>;
}

export interface StorageBackend {
  capabilities(): StorageCapabilities;
  transaction<T>(fn: (tx: StorageTransaction) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}
