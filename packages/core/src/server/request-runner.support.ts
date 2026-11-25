import type { IpcMessage } from '../ipc/protocol.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { parseLine } from '../testing/server-process.ts';
import { DispatcherExecutor } from './dispatcher-executor.ts';
import { Dispatcher } from './dispatcher.ts';
import type { Handler, HandlerOptions } from './dispatcher.ts';
import { RequestRunner } from './request-runner.ts';

export interface RunnerHarness {
  readonly runner: RequestRunner;
  /** Messages the runner handed to `send`, in order. */
  readonly sent: IpcMessage[];
  /** Parsed log lines, in emission order; debug lines included. */
  readonly logs: () => Record<string, unknown>[];
  readonly register: (
    method: string,
    handler: Handler,
    options?: HandlerOptions,
  ) => void;
}

export function startRunner(
  timeoutMs = 1000,
  accepting: () => boolean = () => true,
): RunnerHarness {
  const backend = new MemoryStorageBackend();
  const lines: string[] = [];
  const logger = createJsonLogger({
    // `request completed` and `cancel ignored` are debug lines; the runner's
    // bookkeeping is only observable if the harness keeps them.
    level: 'debug',
    sink: (line) => {
      lines.push(line);
    },
  });
  const dispatcher = new Dispatcher({ backend, timeoutMs, logger });
  const sent: IpcMessage[] = [];
  const runner = new RequestRunner({
    runtime: {
      dispatcher,
      executor: new DispatcherExecutor({ dispatcher }),
      capabilities: backend.capabilities(),
    },
    logger,
    send: (message) => {
      sent.push(message);
    },
    accepting,
  });
  return {
    runner,
    sent,
    logs: () => lines.map(parseLine),
    register: (method, handler, options) => {
      dispatcher.register(method, handler, options);
    },
  };
}
