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
      const { result, sessionChange, events } = await this.dispatcher.dispatch(
        method,
        payload,
        meta,
      );
      // `sessionChange` is present only when the handler changed this request's
      // Session; an unchanged success must not carry an `undefined` field, or
      // the "absent means no change" convention would be indistinguishable from
      // "changed to nothing" further down the IPC path (SPEC 2). Events follow
      // the same convention and stay in process: the request runner sends them
      // after the result, not inside it (ADR-0008).
      const outcome: ExecutorOutcome = { ok: true, result };
      if (sessionChange !== undefined) {
        outcome.sessionChange = sessionChange;
      }
      if (events !== undefined) {
        outcome.events = events;
      }
      return outcome;
    } catch (error) {
      return { ok: false, error: describeFailure(error) };
    }
  }

  onEvent(): () => void {
    // In-process execution returns events with the outcome; the dispatcher has
    // no out-of-band channel, so there is nothing to subscribe to. The method
    // exists so every executor has the same surface (ADR-0008).
    return () => undefined;
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
