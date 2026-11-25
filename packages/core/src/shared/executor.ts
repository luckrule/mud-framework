import type { RequestIdentity } from './identity.ts';

/** Everything an executor needs about a request; all ids are already assigned. */
export interface ExecutorRequest extends RequestIdentity {
  method: string;
  payload: unknown;
  /**
   * The caller's own deadline, which may only tighten the executor's configured
   * ceiling (SPEC 3.4). A remote executor also forwards it, so the other side
   * rolls back at the same instant instead of writing after this side gave up.
   */
  timeoutMs?: number;
  /**
   * Opaque application code version stamped on the Request. `IpcExecutor` fills
   * it from its own configuration when the caller left it unset and keeps it
   * across replays; a caller-supplied value must be a non-empty string, and an
   * invalid one fails the request with `INTERNAL_ERROR` instead of being sent.
   * It is never interpreted by the framework (SPEC 6.4).
   */
  codeVersion?: string;
}

/**
 * A discriminated union rather than a thrown error: across a process boundary
 * the failure is a deserialised `{code, message}`, so `instanceof` cannot work
 * and both sides would need different handling. As a return value, in-process
 * and remote execution follow one path.
 *
 * There is deliberately no category field: the IPC result message carries this
 * type as its own outcome half, so both sides classify through
 * `isFrameworkCode` instead, which is one list rather than two prefix rules.
 */
export type ExecutorOutcome =
  | { ok: true; result: unknown; sessionId?: string }
  | { ok: false; error: { code: string; message: string } };

/**
 * The seam between the connection layer and whoever executes handlers. In
 * process that is `LocalExecutor` over a `Dispatcher`; the two-process design
 * puts an IPC-backed executor behind the same interface.
 */
export interface RequestExecutor {
  /**
   * Brings the executor to a state where `execute` can succeed: a remote
   * executor spawns and handshakes with its Server, an in-process one has
   * nothing to do. Required rather than optional so the compiler forces every
   * caller to start it; a forgotten start used to surface only as requests that
   * silently waited out their deadline (SPEC 6.2 step 3).
   */
  start(): Promise<void>;
  /**
   * MUST settle within the request deadline, never hang, and MUST NOT throw:
   * every failure is returned as `{ok: false, error}`. The connection layer
   * advances a connection's ordered chain only when this promise settles, so a
   * call that never settles because the other side died would stall every later
   * request on that connection (SPEC 3.1, 3.4).
   */
  execute(request: ExecutorRequest): Promise<ExecutorOutcome>;
  /**
   * Ends an accepted request that nobody is waiting for any more (its
   * connection closed, or its deadline passed) and releases it, so a later
   * delivery of the same id is a fresh request rather than a replay. Required
   * for the same reason as `start`: an executor that silently ignored it would
   * keep executing work whose result has no recipient (SPEC 3.6).
   *
   * `code` is the framework error the request ends with; the executor decides
   * how to stop the work, but MUST NOT throw.
   */
  cancel(requestId: string, code: string): Promise<void>;
  /**
   * Releases what this executor owns. In process that closes the dispatcher and
   * through it the storage backend; a remote executor can only ask the other
   * side to shut down, so the interface must not assume it owns storage.
   */
  close(): Promise<void>;
  /**
   * Worst-case duration of `close()` in milliseconds, when the executor can
   * bound it. A remote executor has to drain requests and then wait for the
   * Server to acknowledge, die, or be killed, so its close takes longer than a
   * local one; a caller that applies its own shorter timeout would return while
   * the Server was still writing (the two timeouts used to be unrelated).
   * Optional: an executor without an internal bound leaves the caller's own
   * timeout as the only ceiling.
   */
  readonly closeBudgetMs?: number;
}
