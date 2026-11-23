import { Database } from 'bun:sqlite';
import type { SQLQueryBindings } from 'bun:sqlite';

import type {
  Logger,
  StorageBackend,
  StorageCapabilities,
  StorageTransaction,
} from 'mud-framework-core';

export interface SqliteBackendOptions {
  /** Path to the database file; `':memory:'` (the default) stays in process. */
  filename?: string;
  /** Defaults to `wal` for files and `memory` for `':memory:'`. */
  journalMode?: 'wal' | 'memory';
  busyTimeoutMs?: number;
  maxTransactionDurationMs?: number;
  /**
   * Optional: this package depends on core's types only, never on a logger
   * implementation, so the caller supplies one. Without it nothing is logged.
   */
  logger?: Logger;
}

interface JournalModeRow {
  journal_mode: string;
}

interface TimeoutRow {
  timeout: number;
}

interface ForeignKeysRow {
  foreign_keys: number;
}

const DEFAULT_BUSY_TIMEOUT_MS = 5000;

/**
 * SQLite backend. `bun:sqlite` is fully synchronous, so every statement runs to
 * completion inside the call that issues it; the async facade exists only to
 * satisfy the storage contract. Correctness of `await` points inside a
 * transaction therefore depends on the queue below, not on the driver.
 */
export class SqliteBackend implements StorageBackend {
  private readonly db: Database;
  private readonly declared: StorageCapabilities;
  private readonly requestedJournalMode: 'wal' | 'memory';
  private readonly busyTimeoutMs: number;
  private readonly logger: Logger | undefined;
  private closing = false;
  private closed = false;
  private unusableReason: string | undefined;
  private queue: Promise<unknown> = Promise.resolve();

  private constructor(
    db: Database,
    declared: StorageCapabilities,
    requestedJournalMode: 'wal' | 'memory',
    busyTimeoutMs: number,
    logger: Logger | undefined,
  ) {
    this.db = db;
    this.declared = declared;
    this.requestedJournalMode = requestedJournalMode;
    this.busyTimeoutMs = busyTimeoutMs;
    // Bound once here, not per call: a rollback failure is rare but a PRAGMA
    // check is startup-only, so both may build the binding lazily.
    this.logger = logger?.child({ backend: 'sqlite' });
  }

  static open(options: SqliteBackendOptions = {}): SqliteBackend {
    const filename = options.filename ?? ':memory:';
    const busyTimeoutMs = options.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS;
    const maxTransactionDurationMs =
      options.maxTransactionDurationMs ?? Number.POSITIVE_INFINITY;

    assertNonNegativeInteger(busyTimeoutMs, 'busyTimeoutMs');
    assertPositiveDuration(
      maxTransactionDurationMs,
      'maxTransactionDurationMs',
    );

    const isMemory = filename === ':memory:' || filename === '';
    const journalMode = options.journalMode ?? (isMemory ? 'memory' : 'wal');

    const db = new Database(filename);
    const backend = new SqliteBackend(
      db,
      {
        transactions: 'full',
        concurrency: 'single',
        snapshotRead: true,
        uniqueConstraint: true,
        maxTransactionDurationMs,
      },
      journalMode,
      busyTimeoutMs,
      options.logger,
    );

    try {
      backend.configurePragmas();
    } catch (error) {
      db.close();
      throw error;
    }
    return backend;
  }

  capabilities(): StorageCapabilities {
    this.assertOpen();
    return this.declared;
  }

  transaction<T>(fn: (tx: StorageTransaction) => Promise<T>): Promise<T> {
    this.assertUsable();
    // A single-connection backend must never interleave transactions:
    // concurrent handlers would share one transaction, and one rollback would
    // erase the other's writes without any error. The queue holds until the
    // handler settles, including its await points.
    const result = this.queue.then(() => this.run(fn));
    this.queue = result.catch(() => undefined);
    return result;
  }

  async close(): Promise<void> {
    this.assertOpen();
    this.closing = true;
    // Drain first so close never cuts an in-flight COMMIT in half.
    await this.queue;
    this.db.close(true);
    this.closed = true;
  }

  private async run<T>(fn: (tx: StorageTransaction) => Promise<T>): Promise<T> {
    // Not assertOpen: this transaction was admitted before close() began and
    // close() drains the queue before touching the connection.
    this.assertNotUnusable();
    this.db.run('BEGIN IMMEDIATE');
    try {
      const result = await fn(this.facade());
      this.db.run('COMMIT');
      return result;
    } catch (error) {
      this.rollbackOrInvalidate();
      throw error;
    }
  }

  private facade(): StorageTransaction {
    return {
      query: <T>(
        sql: string,
        params: readonly unknown[] = [],
      ): Promise<T[]> => {
        this.assertInTransaction();
        // `prepare` rather than `query`: the cached statement shares one
        // cursor across callers. Finalize explicitly so long-lived backends
        // do not accumulate native handles.
        const statement = this.db.prepare<T, SQLQueryBindings[]>(sql);
        try {
          return Promise.resolve(statement.all(...toBindings(params)));
        } finally {
          statement.finalize();
        }
      },
      execute: (
        sql: string,
        params: readonly unknown[] = [],
      ): Promise<void> => {
        this.assertInTransaction();
        this.db.run(sql, toBindings(params));
        return Promise.resolve();
      },
    };
  }

  private configurePragmas(): void {
    this.db.run(`PRAGMA journal_mode = ${this.requestedJournalMode}`);
    const journal = this.db
      .query<JournalModeRow, []>('PRAGMA journal_mode')
      .get();
    if (journal?.journal_mode.toLowerCase() !== this.requestedJournalMode) {
      this.logger?.error(
        {
          pragma: 'journal_mode',
          expected: this.requestedJournalMode,
          actual: String(journal?.journal_mode),
        },
        'PRAGMA verification failed',
      );
      throw new Error(
        `SQLite journal_mode is ${String(journal?.journal_mode)} but ` +
          `${this.requestedJournalMode} was requested`,
      );
    }

    this.db.run(`PRAGMA busy_timeout = ${this.busyTimeoutMs}`);
    const timeout = this.db.query<TimeoutRow, []>('PRAGMA busy_timeout').get();
    // Readback key is `timeout`, not `busy_timeout`.
    if (timeout?.timeout !== this.busyTimeoutMs) {
      this.logger?.error(
        {
          pragma: 'busy_timeout',
          expected: this.busyTimeoutMs,
          actual: String(timeout?.timeout),
        },
        'PRAGMA verification failed',
      );
      throw new Error(
        `SQLite busy_timeout is ${String(timeout?.timeout)} but ` +
          `${this.busyTimeoutMs} was requested`,
      );
    }

    this.db.run('PRAGMA foreign_keys = ON');
    const foreignKeys = this.db
      .query<ForeignKeysRow, []>('PRAGMA foreign_keys')
      .get();
    if (foreignKeys?.foreign_keys !== 1) {
      this.logger?.error(
        {
          pragma: 'foreign_keys',
          expected: 1,
          actual: String(foreignKeys?.foreign_keys),
        },
        'PRAGMA verification failed',
      );
      throw new Error('SQLite foreign_keys could not be enabled');
    }
  }

  private rollbackOrInvalidate(): void {
    try {
      this.db.run('ROLLBACK');
    } catch (error) {
      // The connection is still inside a transaction we cannot unwind, so no
      // further statement may run on it.
      this.unusableReason = `rollback failed: ${messageOf(error)}`;
      // error, not warn: the backend is now unusable and every later request
      // fails, which is a failure rather than a degradation (SPEC 7.1).
      this.logger?.error(
        { unusableReason: this.unusableReason },
        'ROLLBACK failed; backend marked unusable',
      );
    }
  }

  private assertOpen(): void {
    if (this.closed || this.closing) {
      throw new Error('storage backend is closed');
    }
  }

  private assertUsable(): void {
    this.assertOpen();
    this.assertNotUnusable();
  }

  private assertNotUnusable(): void {
    if (this.unusableReason !== undefined) {
      throw new Error(`storage backend is unusable: ${this.unusableReason}`);
    }
  }

  /**
   * Guards a transaction handle after its scope has ended: a handler that
   * keeps writing past COMMIT/ROLLBACK would otherwise hit the next
   * transaction's connection.
   */
  private assertInTransaction(): void {
    this.assertNotUnusable();
    if (!this.db.inTransaction) {
      throw new Error('storage transaction is no longer active');
    }
  }
}

function toBindings(params: readonly unknown[]): SQLQueryBindings[] {
  return params.map(toBinding);
}

function toBinding(value: unknown): SQLQueryBindings {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'bigint' ||
    typeof value === 'boolean'
  ) {
    return value;
  }
  if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
    return value;
  }
  throw new TypeError(`unsupported SQLite binding: ${typeof value}`);
}

function assertNonNegativeInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative integer`);
  }
}

function assertPositiveDuration(value: number, name: string): void {
  const valid =
    value === Number.POSITIVE_INFINITY || (Number.isFinite(value) && value > 0);
  if (!valid) {
    throw new RangeError(`${name} must be a positive number or Infinity`);
  }
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
