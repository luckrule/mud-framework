import type {
  StorageBackend,
  StorageCapabilities,
  StorageTransaction,
} from '../storage/types.ts';

export type MemoryQueryHandler = (
  sql: string,
  params: readonly unknown[],
) => unknown[];

export interface MemoryBackendOptions {
  capabilities?: Partial<StorageCapabilities>;
  handler?: MemoryQueryHandler;
}

export interface MemoryBackendStats {
  committed: number;
  rolledBack: number;
}

const DEFAULT_CAPABILITIES: StorageCapabilities = {
  transactions: 'full',
  concurrency: 'pooled',
  snapshotRead: true,
  // Nothing enforces constraints here; claiming true would overstate (SPEC 4.5).
  uniqueConstraint: false,
  maxTransactionDurationMs: Number.POSITIVE_INFINITY,
};

/**
 * Test double for the execution model: it reports transaction outcomes but
 * stores nothing, so `query`/`execute` delegate to an injected handler instead
 * of pretending to parse SQL (SPEC 4.5 requires framework tests to use it).
 */
export class MemoryStorageBackend implements StorageBackend {
  private readonly declared: StorageCapabilities;
  private readonly handler: MemoryQueryHandler;
  private closed = false;
  private committed = 0;
  private rolledBack = 0;
  private queue: Promise<unknown> = Promise.resolve();

  constructor(options: MemoryBackendOptions = {}) {
    this.declared = { ...DEFAULT_CAPABILITIES, ...options.capabilities };
    this.handler = options.handler ?? (() => []);
  }

  capabilities(): StorageCapabilities {
    this.assertOpen();
    return this.declared;
  }

  transaction<T>(fn: (tx: StorageTransaction) => Promise<T>): Promise<T> {
    this.assertOpen();
    if (this.declared.concurrency !== 'single') {
      return this.run(fn);
    }
    // A 'single' backend must never interleave transactions (INV-017).
    const result = this.queue.then(() => this.run(fn));
    this.queue = result.catch(() => undefined);
    return result;
  }

  async close(): Promise<void> {
    this.assertOpen();
    this.closed = true;
  }

  stats(): MemoryBackendStats {
    return { committed: this.committed, rolledBack: this.rolledBack };
  }

  private async run<T>(fn: (tx: StorageTransaction) => Promise<T>): Promise<T> {
    try {
      const result = await fn(this.facade());
      this.committed += 1;
      return result;
    } catch (error) {
      this.rolledBack += 1;
      throw error;
    }
  }

  private facade(): StorageTransaction {
    return {
      query: <T>(
        sql: string,
        params: readonly unknown[] = [],
      ): Promise<T[]> => {
        // Nothing is persisted, so no row shape exists to narrow against.
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- untyped test double
        const rows = this.handler(sql, params) as T[];
        return Promise.resolve(rows);
      },
      execute: async (
        sql: string,
        params: readonly unknown[] = [],
      ): Promise<void> => {
        this.handler(sql, params);
      },
    };
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new Error('storage backend is closed');
    }
  }
}
