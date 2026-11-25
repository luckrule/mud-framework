import type {
  ExecutorHealth,
  ExecutorOutcome,
  ExecutorRequest,
  RequestExecutor,
} from '../shared/executor.ts';
import { describeFailure } from '../shared/wire-codes.ts';
import type { Dispatcher } from './dispatcher.ts';

export interface DispatcherExecutorOptions {
  dispatcher: Dispatcher;
}

/**
 * Adapts the Server's `Dispatcher` to the `RequestExecutor` contract its
 * request runner consumes. It exists only inside the Server process, which
 * already owns the backend and the handlers; the Connection process reaches
 * handlers through `IpcExecutor` and never constructs this.
 */
export class DispatcherExecutor implements RequestExecutor {
  private readonly dispatcher: Dispatcher;

  constructor(options: DispatcherExecutorOptions) {
    this.dispatcher = options.dispatcher;
  }

  start(): Promise<void> {
    // The dispatcher is ready when it is constructed: nothing to spawn,
    // nothing to handshake.
    return Promise.resolve();
  }

  async execute(request: ExecutorRequest): Promise<ExecutorOutcome> {
    try {
      // Everything except the handler arguments is dispatch metadata, so the
      // split is expressed once here instead of field by field.
      const { method, payload, ...meta } = request;
      const { result, sessionChange } = await this.dispatcher.dispatch(
        method,
        payload,
        meta,
      );
      // `sessionChange` is present only when the handler changed this request's
      // Session; an unchanged success must not carry an `undefined` field, or
      // the "absent means no change" convention would be indistinguishable from
      // "changed to nothing" further down the IPC path (SPEC 2).
      return sessionChange === undefined
        ? { ok: true, result }
        : { ok: true, result, sessionChange };
    } catch (error) {
      return { ok: false, error: describeFailure(error) };
    }
  }

  cancel(requestId: string, code: string): Promise<void> {
    // Synchronous here: the dispatcher only has to mark the expiry and settle
    // the pending promise, both of which happen before it returns.
    this.dispatcher.cancel(requestId, code);
    return Promise.resolve();
  }

  close(): Promise<void> {
    return this.dispatcher.close();
  }

  health(): ExecutorHealth {
    return this.dispatcher.isClosed()
      ? { state: 'closed', ready: false }
      : { state: 'ready', ready: true };
  }
}
