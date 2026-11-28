import type { ServerEvent } from '../shared/events.ts';
import type {
  ExecutorRequest,
  RequestExecutor,
  SessionChange,
} from '../shared/executor.ts';

export interface ControlledExecutor {
  executor: RequestExecutor;
  /** Methods whose execution started, in order. */
  started: string[];
  /** Methods whose execution returned, in order. */
  finished: string[];
  /** Every `ExecutorRequest` the layer handed over, in call order. */
  requests: ExecutorRequest[];
  /** Settles every `hang` call that is waiting. */
  release: () => void;
  /** Pushes one Server event to the registered sink (ADR-0008). */
  emit: (event: ServerEvent) => void;
  /** How many sinks the layer currently has registered. */
  listenerCount: () => number;
}

/**
 * An executor that reports a Session change for the methods the options name
 * (`sessionOf` binds, `changeOf` can return either change or none), hangs on
 * `hang` until released, and succeeds immediately otherwise. `cancel` is
 * deliberately a no-op: the reconnect tests need a request that outlives its
 * connection, so the layer's own bounded waits are what get measured rather
 * than the executor's.
 */
export function controlledExecutor(
  options: {
    sessionOf?: (method: string) => string | undefined;
    changeOf?: (method: string) => SessionChange | undefined;
  } = {},
): ControlledExecutor {
  const started: string[] = [];
  const finished: string[] = [];
  const requests: ExecutorRequest[] = [];
  const eventListeners = new Set<(event: ServerEvent) => void>();
  const hang = Promise.withResolvers<void>();
  return {
    started,
    finished,
    requests,
    release: () => {
      hang.resolve();
    },
    emit: (event) => {
      for (const listener of new Set(eventListeners)) {
        listener(event);
      }
    },
    listenerCount: () => eventListeners.size,
    executor: {
      start: () => Promise.resolve(),
      execute: async (request) => {
        started.push(request.method);
        requests.push(request);
        if (request.method === 'hang') {
          await hang.promise;
        }
        finished.push(request.method);
        const sessionId = options.sessionOf?.(request.method);
        const sessionChange =
          options.changeOf !== undefined
            ? options.changeOf(request.method)
            : sessionId === undefined
              ? undefined
              : { type: 'bind' as const, sessionId };
        return sessionChange === undefined
          ? { ok: true, result: request.method }
          : { ok: true, result: request.method, sessionChange };
      },
      onEvent: (listener) => {
        eventListeners.add(listener);
        return () => {
          eventListeners.delete(listener);
        };
      },
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
    },
  };
}
