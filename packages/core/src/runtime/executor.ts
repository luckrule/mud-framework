import type { Dispatcher } from './dispatcher.ts';
import { describeFailure } from './wire-codes.ts';

/** Everything an executor needs about a request; all ids are already assigned. */
export interface ExecutorRequest {
  /** Minted by the connection layer on entry (SPEC 7.3); MUST NOT be replaced. */
  requestId: string;
  connectionId: string;
  clientRequestId: string;
  sessionId?: string;
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
 * A discriminated union rather than a thrown error: across a process boundary
 * the failure is a deserialised `{code, message}`, so `instanceof` cannot work
 * and both sides would need different handling. As a return value, in-process
 * and remote execution follow one path.
 *
 * There is deliberately no category field: the IPC `ResultError` is `{code,
 * message}` and the protocol is not this module's to widen. Both processes
 * classify through `isFrameworkCode` instead, which is one list rather than two
 * prefix rules.
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

export interface LocalExecutorOptions {
  dispatcher: Dispatcher;
}

export class LocalExecutor implements RequestExecutor {
  private readonly dispatcher: Dispatcher;

  constructor(options: LocalExecutorOptions) {
    this.dispatcher = options.dispatcher;
  }

  start(): Promise<void> {
    // In process the dispatcher is ready when it is constructed: nothing to
    // spawn, nothing to handshake.
    return Promise.resolve();
  }

  async execute(request: ExecutorRequest): Promise<ExecutorOutcome> {
    try {
      const { result, sessionId } = await this.dispatcher.dispatch(
        request.method,
        request.payload,
        {
          requestId: request.requestId,
          connectionId: request.connectionId,
          clientRequestId: request.clientRequestId,
          sessionId: request.sessionId,
          timeoutMs: request.timeoutMs,
        },
      );
      // `sessionId` is present only when the handler bound this request to a
      // Session; an unbound success must not carry an `undefined` field, or
      // the "absent means unbound" convention would be indistinguishable from
      // "bound to nothing" further down the IPC path (SPEC 2).
      return sessionId === undefined
        ? { ok: true, result }
        : { ok: true, result, sessionId };
    } catch (error) {
      return { ok: false, error: describeFailure(error) };
    }
  }

  cancel(requestId: string, code: string): Promise<void> {
    // Synchronous in process: the dispatcher only has to mark the expiry and
    // settle the pending promise, both of which happen before it returns.
    this.dispatcher.cancel(requestId, code);
    return Promise.resolve();
  }

  close(): Promise<void> {
    return this.dispatcher.close();
  }
}
