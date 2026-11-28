import { Database } from 'bun:sqlite';
import type { SQLQueryBindings } from 'bun:sqlite';

import { safeLogger } from 'mud-framework-core';
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
   * Optional: this package depends on core's types and the stateless
   * `safeLogger` wrapper only, never on a logger implementation; the caller
   * supplies the implementation. Without it nothing is logged.
   */
  logger?: Logger;
}

interface PragmaCheck {
  readonly pragma: string;
  readonly set: string;
  readonly read: string;
  /** Readback column; `busy_timeout` reports `timeout`, not its own name. */
  readonly key: string;
  readonly expected: string | number;
  /** `journal_mode` casing is not part of the contract. */
  readonly foldCase: boolean;
  /** Built from the raw readback, so the message keeps the value SQLite returned. */
  readonly failure: (actual: string | number | undefined) => string;
}

interface TransactionScope {
  active: boolean;
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
  private closePromise: Promise<void> | undefined;
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
    this.logger =
      logger === undefined
        ? undefined
        : safeLogger(logger).child({ backend: 'sqlite' });
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

  /**
   * Memoised and idempotent: a second close joins the first attempt instead of
   * throwing on `closing`; a failed `db.close` keeps the returned promise
   * rejected and observable rather than being replaced by a later "already
   * closed" success. There is no retry.
   */
  close(): Promise<void> {
    this.closePromise ??= this.doClose();
    return this.closePromise;
  }

  private async doClose(): Promise<void> {
    this.closing = true;
    // Drain first so close never cuts an in-flight COMMIT in half.
    await this.queue;
    this.db.close(true);
  }

  private async run<T>(fn: (tx: StorageTransaction) => Promise<T>): Promise<T> {
    // Not assertOpen: this transaction was admitted before close() began and
    // close() drains the queue before touching the connection.
    this.assertNotUnusable();
    this.db.run('BEGIN IMMEDIATE');
    const scope: TransactionScope = { active: true };
    try {
      const result = await fn(this.facade(scope));
      this.db.run('COMMIT');
      return result;
    } catch (error) {
      this.rollbackOrInvalidate();
      throw error;
    } finally {
      scope.active = false;
    }
  }

  private facade(scope: TransactionScope): StorageTransaction {
    return {
      query: <T>(
        sql: string,
        params: readonly unknown[] = [],
      ): Promise<T[]> => {
        this.assertInTransaction(scope);
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
        this.assertInTransaction(scope);
        this.db.run(sql, toBindings(params));
        return Promise.resolve();
      },
    };
  }

  private configurePragmas(): void {
    const checks: readonly PragmaCheck[] = [
      {
        pragma: 'journal_mode',
        set: `PRAGMA journal_mode = ${this.requestedJournalMode}`,
        read: 'PRAGMA journal_mode',
        key: 'journal_mode',
        expected: this.requestedJournalMode,
        foldCase: true,
        failure: (actual) =>
          `SQLite journal_mode is ${String(actual)} but ` +
          `${this.requestedJournalMode} was requested`,
      },
      {
        pragma: 'busy_timeout',
        set: `PRAGMA busy_timeout = ${this.busyTimeoutMs}`,
        read: 'PRAGMA busy_timeout',
        key: 'timeout',
        expected: this.busyTimeoutMs,
        foldCase: false,
        failure: (actual) =>
          `SQLite busy_timeout is ${String(actual)} but ` +
          `${this.busyTimeoutMs} was requested`,
      },
      {
        pragma: 'foreign_keys',
        set: 'PRAGMA foreign_keys = ON',
        read: 'PRAGMA foreign_keys',
        key: 'foreign_keys',
        expected: 1,
        foldCase: false,
        failure: () => 'SQLite foreign_keys could not be enabled',
      },
    ];
    for (const check of checks) {
      this.db.run(check.set);
      const row = this.db
        .query<Record<string, string | number>, []>(check.read)
        .get();
      const raw = row?.[check.key];
      this.requirePragma(
        check.pragma,
        check.expected,
        raw,
        check.foldCase,
        check.failure(raw),
      );
    }
  }

  private requirePragma(
    pragma: string,
    expected: string | number,
    actual: string | number | undefined,
    foldCase: boolean,
    failure: string,
  ): void {
    const matches =
      foldCase && typeof actual === 'string'
        ? actual.toLowerCase() === expected
        : actual === expected;
    if (matches) {
      return;
    }
    this.logger?.error(
      { pragma, expected, actual: String(actual) },
      'PRAGMA verification failed',
    );
    throw new Error(failure);
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
    if (this.closing) {
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
  private assertInTransaction(scope: TransactionScope): void {
    this.assertNotUnusable();
    if (!scope.active || !this.db.inTransaction) {
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
