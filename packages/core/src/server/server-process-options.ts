import type { IpcChannel } from '../ipc/channel.ts';
import type { Logger } from '../logging/types.ts';
import type { StorageBackend } from '../storage/types.ts';
import type { DispatcherOptions } from './dispatcher.ts';
import type { Extension } from './extension.ts';

export interface ServerProcessOptions {
  /** Builds the storage backend. Called once per process start. */
  createBackend: () => Promise<StorageBackend> | StorageBackend;
  /**
   * Extensions in registration order. Each is checked against the backend
   * before its `register` runs (SPEC 4.3).
   */
  extensions: readonly Extension[];
  /** Framework-side request ceiling; effective = min(this, backend max). */
  timeoutMs: number;
  /**
   * How many events one request may buffer (SPEC 3.8). `ctx.emit` and
   * `ctx.emitToSession` share the budget, which resets for each request.
   * Defaults to 1024. Must be a safe integer of at least 1; an invalid value
   * refuses startup. The overflow is an ordinary handler error: caught, the
   * request can still succeed; uncaught, it fails with `INTERNAL_ERROR`.
   */
  maxEventsPerRequest?: number;
  /**
   * Optional access policy for every registered handler (SPEC 3.7). Forwarded
   * unchanged to the dispatcher this process creates; returning false refuses
   * a request with `FORBIDDEN` before its transaction opens.
   */
  authorize?: DispatcherOptions['authorize'];
  logger?: Logger;
  /**
   * The IPC channel; defaults to the real process channel.
   *
   * At most one `runServerProcess` per OS process may use the real channel:
   * `createProcessChannel` is a process-level singleton and a second one throws.
   * Two Servers in one process would receive each other's requests and both
   * write to storage (INV-002).
   */
  channel?: IpcChannel;
  /**
   * Overrides the budget for each abort step (see `ABORT_TIMEOUT_MS`). Exists
   * for tests, which would otherwise have to wait the real 5s; a deployment has
   * no reason to set it.
   */
  abortTimeoutMs?: number;
  /**
   * Overrides the parent-liveness poll interval. 0 disables the poll, which is
   * what a test harness with an in-memory channel needs: there is no real
   * parent to watch, and a background timer would outlive the test.
   */
  parentCheckIntervalMs?: number;
  /**
   * Reads the current parent pid. Defaults to `process.ppid`; injectable so a
   * test can simulate a reparented process, which cannot be done for real from
   * inside the test process.
   */
  readParentPid?: () => number;
}
