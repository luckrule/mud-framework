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
}

/**
 * A Session change a handler declared and the framework applied only after the
 * request succeeded (SPEC 2). Unbind is a separate member rather than a null
 * id: "this request ended the binding" and "this request bound nothing" are
 * different outcomes, and a failed request must produce neither.
 */
export type SessionChange =
  | { readonly type: 'bind'; readonly sessionId: string }
  | { readonly type: 'unbind' };

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
  | { ok: true; result: unknown; sessionChange?: SessionChange }
  | { ok: false; error: { code: string; message: string } };

/**
 * What an executor reports about its own ability to execute now. `recovering`
 * covers a restart or a pending replacement; `blocked` means recovery is
 * stopped by an unconfirmed writer exit (INV-002); `unknown` is the fallback a
 * caller uses when the executor cannot answer.
 */
export type ExecutorHealthState =
  | 'idle'
  | 'starting'
  | 'ready'
  | 'recovering'
  | 'blocked'
  | 'closed'
  | 'unknown';

export interface ExecutorHealth {
  readonly state: ExecutorHealthState;
  /** True only when the executor can execute now, without waiting for recovery. */
  readonly ready: boolean;
}

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
   * A synchronous, side-effect-free snapshot of whether `execute` can run now
   * (SPEC 6.6). Callers use it for deployment probes; it is advisory and may be
   * stale immediately, so it MUST NOT gate an individual request. It must be
   * safe before `start()`, during `stop()` and while recovering, and must not
   * throw or log on every poll: a failing implementation is reported as
   * `{state:'unknown', ready:false}` by the caller.
   */
  health(): ExecutorHealth;
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
