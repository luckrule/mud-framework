import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import { Dispatcher } from '../runtime/dispatcher.ts';
import type {
  StorageBackend,
  StorageCapabilities,
  StorageTransaction,
} from '../storage/types.ts';
import { isRecord, sleep } from './common.ts';

/** Parsed log lines, in emission order. */
export function parseLines(
  lines: readonly string[],
): Record<string, unknown>[] {
  return lines.map((line) => {
    const parsed: unknown = JSON.parse(line);
    if (!isRecord(parsed)) {
      throw new Error(`log line is not a JSON object: ${line}`);
    }
    return parsed;
  });
}

/** Dispatcher whose logs go to `lines` instead of stdout. */
export function quietDispatcher(
  options: ConstructorParameters<typeof Dispatcher>[0],
  lines: string[] = [],
): { dispatcher: Dispatcher; lines: string[] } {
  const logger: Logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  return {
    dispatcher: new Dispatcher({ ...options, logger }),
    lines,
  };
}

export function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

/**
 * A backend whose COMMIT lands a macrotask after the handler returns, like a
 * real database flushing its log. `MemoryStorageBackend` commits in the same
 * turn, which hides the window where a cancel arrives too late to roll back.
 */
export class SlowCommitBackend implements StorageBackend {
  private committed = 0;
  private rolledBack = 0;

  capabilities(): StorageCapabilities {
    return {
      transactions: 'full',
      concurrency: 'pooled',
      snapshotRead: true,
      uniqueConstraint: false,
      maxTransactionDurationMs: Number.POSITIVE_INFINITY,
    };
  }

  async transaction<T>(fn: (tx: StorageTransaction) => Promise<T>): Promise<T> {
    const tx: StorageTransaction = {
      query: <R>(): Promise<R[]> => Promise.resolve([]),
      execute: (): Promise<void> => Promise.resolve(),
    };
    let result: T;
    try {
      result = await fn(tx);
    } catch (error) {
      this.rolledBack += 1;
      throw error;
    }
    await sleep(0);
    this.committed += 1;
    return result;
  }

  close(): Promise<void> {
    return Promise.resolve();
  }

  stats(): { committed: number; rolledBack: number } {
    return { committed: this.committed, rolledBack: this.rolledBack };
  }
}
