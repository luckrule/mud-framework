import type { IpcChannel } from '../ipc/channel.ts';
import { readyMessage } from '../ipc/outbound.ts';
import type { IpcMessage, ShutdownMessage } from '../ipc/protocol.ts';
import type { Logger } from '../logging/types.ts';
import { RequestRunner } from './request-runner.ts';
import type { ServerProcessOptions } from './server-process-options.ts';
import type { ServerShutdown } from './server-shutdown.ts';
import type { Runtime } from './server-startup.ts';
import { startServer } from './server-startup.ts';

export interface ActivationDeps {
  readonly options: ServerProcessOptions;
  readonly logger: Logger;
  readonly channel: IpcChannel;
  readonly shutdown: ServerShutdown;
  readonly activation: PromiseWithResolvers<void>;
  /** Enters `starting`. */
  readonly beginStarting: () => void;
  /** Enters `active`; the readiness barrier is sent immediately after. */
  readonly activate: () => void;
  readonly enterShuttingDown: () => void;
  readonly isShuttingDown: () => boolean;
  readonly isActive: () => boolean;
  /** Records the running pieces so the liveness paths can reach them. */
  readonly attach: (runtime: Runtime, runner: RequestRunner) => void;
  readonly pendingShutdown: () => ShutdownMessage | undefined;
  readonly channelClosedReason: () => string | undefined;
  readonly parentGoneReason: () => string | undefined;
  readonly sendBarrier: (message: IpcMessage, what: string) => void;
  readonly sendQuietly: (message: IpcMessage, what: string) => void;
  readonly onMessage: (raw: unknown) => void;
  readonly onChannelClose: (reason: string) => void;
  /** The process-level error a dead link is reported as. */
  readonly channelClosedError: (reason: string) => Error;
}

/**
 * Runs once `activate` arrives: opens the runtime through `startServer`
 * (SPEC 6.2), attaches the request handlers, marks the process active and
 * only then sends `ready`. The pre-activation handlers stay installed until
 * the new ones are in place, so a request that arrives during the start is
 * ignored instead of meeting a half-built Server.
 */
export async function startAndServe(deps: ActivationDeps): Promise<void> {
  const { options, logger, channel } = deps;
  deps.beginStarting();
  let runtime: Runtime;
  try {
    ({ runtime } = await startServer({ options, logger }));
  } catch (error) {
    // `startServer` closed whatever it opened. A link that died during the
    // start is reported as what it is: the peer is gone, not that the
    // configuration was refused. The same holds for a parent that vanished
    // mid-start.
    const gone = deps.channelClosedReason() ?? deps.parentGoneReason();
    throw gone === undefined ? error : deps.channelClosedError(gone);
  }
  const runner = new RequestRunner({
    runtime,
    logger,
    send: (message, what) => deps.sendQuietly(message, what),
    // The phase stays in the facade: `shutdown` owns it, and the runner only
    // asks.
    accepting: () => deps.isActive(),
  });
  deps.attach(runtime, runner);
  try {
    channel.onMessage((raw) => {
      deps.onMessage(raw);
    });
    channel.onClose((reason) => {
      deps.onChannelClose(reason);
    });
  } catch (error) {
    // The channel is outside code: a registration that throws must not leave
    // the backend open. The original error is the one worth reporting, so a
    // failure to close does not replace it.
    await runtime.executor.close().catch(() => undefined);
    throw error;
  }
  // Registration above can deliver what arrived while this was starting. A
  // dead link wins over a parked shutdown: nobody can receive `closed`, so
  // the close has to be an abort. Both are applied before the phase check,
  // because a shutdown parked during `startServer` already set the phase and
  // would otherwise be left without a drain.
  const channelClosedReason = deps.channelClosedReason();
  if (channelClosedReason !== undefined) {
    deps.enterShuttingDown();
    deps.activation.resolve();
    void deps.shutdown.abort(runtime, runner, channelClosedReason);
    return;
  }
  const parentGoneReason = deps.parentGoneReason();
  if (parentGoneReason !== undefined) {
    // The poll fired during the start. Nobody is left to announce readiness
    // to, and the backend this start just opened must be closed here: the
    // poll could not do it because the runtime did not exist yet.
    deps.enterShuttingDown();
    deps.activation.resolve();
    void deps.shutdown.abort(runtime, runner, parentGoneReason);
    return;
  }
  const pendingShutdown = deps.pendingShutdown();
  if (pendingShutdown !== undefined) {
    deps.activation.resolve();
    deps.shutdown.begin(runtime, runner, pendingShutdown);
    return;
  }
  if (deps.isShuttingDown()) {
    // A `shutdown` delivered at registration found the runtime and started
    // its own drain; there is nothing left to begin.
    deps.activation.resolve();
    return;
  }
  // SPEC 6.2 step 3, in this order and no other: the handler is installed, the
  // phase says requests may run, and only then is the barrier crossed. A
  // Portal is entitled to deliver the instant it sees `ready`, so anything
  // sent before this point would arrive with nowhere to go and be dropped.
  deps.activate();
  try {
    deps.sendBarrier(readyMessage(process.pid, runtime.capabilities), 'ready');
  } catch (error) {
    // The barrier was never crossed, so this process is not a Server anyone
    // can use: leaving the backend open would keep a writer alive beside the
    // replacement (INV-002), and leaving the phase `active` would promise work
    // this process cannot deliver. Rejecting the activation is what makes
    // `run()` end as a startup failure instead of parking forever.
    //
    // The drain is here for the channel that delivers `ready` and only then
    // throws: a request flushed by that delivery may already be running, and
    // the backend must not close under it.
    deps.enterShuttingDown();
    await deps.shutdown.drainAndClose(
      runtime,
      runner,
      deps.shutdown.abortBudgetMs,
    );
    deps.activation.reject(error);
    return;
  }
  logger.info(
    { pid: process.pid, capabilities: runtime.capabilities },
    'server process ready',
  );
  deps.activation.resolve();
}
