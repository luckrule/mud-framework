import type { IpcChannel } from '../../ipc/channel.ts';
import { IpcChannelClosedError } from '../../ipc/channel.ts';
import type { SpawnedServer } from '../../ipc/spawn-port.ts';
import { harness, messagesOf } from '../../testing/ipc-executor.ts';
import type { FakeServerProcess, Harness } from '../../testing/ipc-executor.ts';

export const SWITCH_COMPLETED = 'server switch completed';
export const SWITCH_FAILED =
  'server switch failed; the previous server keeps serving';
export const SWITCH_ROLLBACK =
  'server switch failed after the previous server retired; restarting the previous version';
export const SWITCH_LOST =
  'server switch failed because the previous server was lost; restarting the previous version';
/** The replacement's pid, distinct from the initial process's 4242. */
export const NEW_PID = 5252;

export function messagesTo(server: FakeServerProcess): string[] {
  return messagesOf(server).map((message) => message.type);
}

/** One process per generation, with a shared timeline for ordering checks. */
export function switchable(
  options: {
    timeoutMs?: number;
    shutdownTimeoutMs?: number;
    startTimeoutMs?: number;
    restartBackoffMs?: number;
    exitOnShutdown?: boolean;
    holdRequests?: boolean;
    readyOnActivate?: boolean;
    killExits?: boolean;
  } = {},
): Harness & { timeline: string[] } {
  const timeline: string[] = [];
  const h = harness({
    timeoutMs: options.timeoutMs ?? 5000,
    shutdownTimeoutMs: options.shutdownTimeoutMs ?? 50,
    startTimeoutMs: options.startTimeoutMs ?? 500,
    restartBackoffMs: options.restartBackoffMs,
    exitOnShutdown: options.exitOnShutdown,
    holdRequests: options.holdRequests,
    readyOnActivate: options.readyOnActivate,
    killExits: options.killExits,
    timeline,
    label: 'old',
  });
  return { ...h, timeline };
}

export function labelledServer(
  h: Harness & { timeline: string[] },
  label: string,
  overrides: {
    readyOnActivate?: boolean;
    silentOnHello?: boolean;
    exitOnShutdown?: boolean;
    killExits?: boolean;
    pid?: number;
    crashOnActivate?: boolean;
    stderrTail?: readonly string[];
  } = {},
): SpawnedServer {
  return h.makeServer({
    label,
    // A distinct pid keeps every assertion that names a process pointing at
    // the replacement rather than accidentally matching the old one.
    pid: overrides.pid ?? NEW_PID,
    readyOnActivate: overrides.readyOnActivate ?? true,
    silentOnHello: overrides.silentOnHello,
    exitOnShutdown: overrides.exitOnShutdown ?? true,
    // Absent, not `undefined`: an explicit key would shadow the harness default.
    ...(overrides.killExits === undefined
      ? {}
      : { killExits: overrides.killExits }),
    crashOnActivate: overrides.crashOnActivate,
    stderrTail: overrides.stderrTail,
    timeline: h.timeline,
  });
}

/**
 * Wraps a replacement so that its first delivery after `activate` fails on the
 * wire: the process is promoted and then found dead on the first `flush()`.
 * Only requests fail; the handshake itself stays intact.
 */
export function failFirstDeliveryAfterActivate(
  handle: SpawnedServer,
): SpawnedServer {
  let activated = false;
  let failed = false;
  const channel: IpcChannel = {
    send(message): void {
      if (message.type === 'activate') {
        activated = true;
        handle.channel.send(message);
        return;
      }
      if (activated && !failed && message.type === 'request') {
        failed = true;
        throw new IpcChannelClosedError('test delivery failed');
      }
      handle.channel.send(message);
    },
    onMessage: (handler) => {
      handle.channel.onMessage(handler);
    },
    onClose: (handler) => {
      handle.channel.onClose(handler);
    },
    close: (reason) => {
      handle.channel.close(reason);
    },
  };
  return { ...handle, channel };
}
