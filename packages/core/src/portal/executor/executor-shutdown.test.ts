import { describe, expect, test } from 'bun:test';

import type { IpcChannel } from '../../ipc/channel.ts';
import type { IpcMessage } from '../../ipc/protocol.ts';
import type { ServerExitInfo } from '../../ipc/spawn-port.ts';
import { createJsonLogger } from '../../logging/json-logger.ts';
import { flushMicrotasks } from '../../testing/ipc-executor.ts';
import type { ShutdownHost } from './executor-shutdown.ts';
import { doClose } from './executor-shutdown.ts';
import { ServerSlot } from './server-slot.ts';
import type { ServerState } from './server-slot.ts';
import type { SwitchOperation } from './version-switch.ts';

const OBSERVED_EXIT: ServerExitInfo = { code: 0, signal: null, observed: true };

/** A channel that records the message types the Portal sent to one process. */
function recordingChannel(sent: IpcMessage[]): IpcChannel {
  return {
    send(message): void {
      sent.push(message);
    },
    onMessage: () => {},
    onClose: () => {},
    close: () => {},
  };
}

/**
 * A `ServerState` with every field present, so a test names only the ones it
 * depends on. The real `doClose` reads `ready`, `closed`, `shutdownSent`,
 * `shutdownAcknowledged`, `exited` and `exitPromise`.
 */
function serverState(overrides: Partial<ServerState> = {}): ServerState {
  return {
    generation: 1,
    channel: recordingChannel([]),
    kill: () => {},
    exitPromise: Promise.resolve(OBSERVED_EXIT),
    lastExit: () => OBSERVED_EXIT,
    ready: true,
    closed: false,
    exited: false,
    shutdownAcknowledged: false,
    pid: 1001,
    staged: false,
    retiring: false,
    declaredMaxDurationMs: undefined,
    shutdownSent: false,
    closedSignal: Promise.withResolvers<void>(),
    ...overrides,
  };
}

describe('executor shutdown', () => {
  test('shuts the current server down before confirming an abandoned replacement exit', async () => {
    // The step order in `doClose` is the contract, not an implementation detail:
    // the current Server is shut down, and only then is the abandoned
    // replacement's exit confirmed. Swapping the two makes `close()` wait on a
    // possibly lingering replacement before it even asked the current Server to
    // stop. The integration cases only assert the final kill/exit state, so they
    // stay green under that swap (the real orchestration never has a live current
    // Server next to an activated abandoned one). This pins the order at the
    // `doClose` seam, where both steps are observable.
    const order: string[] = [];
    const slot = new ServerSlot<SwitchOperation>();
    slot.beginStart();

    const currentExit = Promise.withResolvers<ServerExitInfo>();
    let current: ServerState;
    const currentSent: IpcMessage[] = [];
    const currentChannel: IpcChannel = {
      send(message): void {
        currentSent.push(message);
        if (message.type === 'shutdown') {
          order.push('current:shutdown');
          // Model a cooperative Server: it acknowledges and exits at once, so
          // `shutdownServer` does not spend its grace or its kill.
          current.closed = true;
          current.shutdownAcknowledged = true;
          current.closedSignal.resolve();
          currentExit.resolve(OBSERVED_EXIT);
        }
      },
      onMessage: () => {},
      onClose: () => {},
      close: () => {},
    };
    current = serverState({
      channel: currentChannel,
      exitPromise: currentExit.promise,
      pid: 1001,
    });
    slot.trackSpawning(current);

    const abandonedExit = Promise.withResolvers<ServerExitInfo>();
    const abandoned = serverState({
      exitPromise: abandonedExit.promise,
      pid: 2002,
      staged: false,
    });
    slot.track(abandoned);
    void abandonedExit.promise.then(() => {
      order.push('abandoned:exit');
      return undefined;
    });

    const host: ShutdownHost = {
      logger: createJsonLogger({ level: 'debug', sink: () => {} }),
      slot,
      shutdownTimeoutMs: 50,
      versionSwitch: { abortSwitchForClose: async () => {} },
      ledger: {
        failUndelivered: () => {},
        awaitPendingSettled: async () => {},
        failAllPending: () => {},
        pendingCount: () => 0,
      },
      restart: { clearRestartTimer: () => {}, clearStableClock: () => {} },
      waiters: {
        clearStartTimer: () => {},
        rejectStart: () => {},
        waitForClosed: async () => {},
      },
      listPendingExits: () => slot.pendingExits,
    };

    const closing = doClose(host);
    let observedBeforeExit: string[] = [];
    try {
      await flushMicrotasks();
      observedBeforeExit = [...order];
      expect(host.listPendingExits()).toEqual([abandoned]);
      expect(host.listPendingExits()).toEqual([abandoned]);
      expect(slot.pendingExits).toEqual([abandoned]);
    } finally {
      // Unblock `confirmAbandonedExits` whichever order ran, so the close always
      // settles and the assertion below reports the real ordering.
      abandonedExit.resolve(OBSERVED_EXIT);
      await closing;
    }

    // The current Server was already told to stop while the abandoned
    // replacement was still unconfirmed: `shutdownServer` ran first.
    expect(observedBeforeExit).toEqual(['current:shutdown']);
    expect(currentSent).toContainEqual({ type: 'shutdown', timeoutMs: 50 });
    expect(order).toEqual(['current:shutdown', 'abandoned:exit']);
    expect(slot.status).toBe('closed');
    expect(host.listPendingExits()).toEqual([]);
    expect(slot.pendingExits).toEqual([]);
  });
});
