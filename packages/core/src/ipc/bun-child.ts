import { createJsonLogger } from '../logging/json-logger.ts';
import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import { messageOf } from '../shared/guards.ts';
import type { IpcChannel } from './channel.ts';
import { IpcChannelClosedError } from './channel.ts';
import type { IpcMessage } from './protocol.ts';

export interface ProcessChannelOptions {
  /** Defaults to the JSON logger; only used when IPC is unavailable. */
  logger?: Logger;
}

const DEFAULT_CLOSE_REASON = 'closed by local side';

/**
 * The one channel this process owns, if any. `process.send` and the
 * `message`/`disconnect` events are process-global, so a second channel would
 * add a second listener and deliver every inbound message to both — the
 * two-writer defect INV-002 exists to prevent. A process therefore has at most
 * one channel, and a second construction is a modelling error, not a case to
 * support.
 */
let processChannel: IpcChannel | undefined;

/**
 * The Server side of the real Portal <-> Server link: `process.send` plus the
 * `message`/`disconnect` events of a Bun child process (ADR-0002).
 *
 * At most one per process: a second call throws, because a process has exactly
 * one IPC link to its parent (`process.send` is process-global). Two Server
 * processes in one OS process is the configuration this rules out; run them as
 * separate processes instead.
 *
 * The `message` listener is attached at construction rather than at
 * `onMessage`, because the runtime drops a message that has no listener; the
 * buffer keeps the same "delivered at registration" contract the in-memory
 * pair gives (channel.ts).
 */
export function createProcessChannel(
  options: ProcessChannelOptions = {},
): IpcChannel {
  if (processChannel !== undefined) {
    throw new Error(
      'a process channel already exists: a process has exactly one IPC link to its parent, so run each Server process in its own OS process',
    );
  }
  const logger = safeLogger(
    options.logger ??
      createJsonLogger({ bindings: { component: 'bun-child' } }),
  );
  const hasProcessSend = typeof process.send === 'function';
  if (!hasProcessSend) {
    logger.warn(
      {
        code: 'IPC_UNAVAILABLE',
        reason: 'process.send is undefined; the process has no IPC channel',
      },
      'ipc channel unavailable',
    );
  }

  let closedReason: string | undefined;
  let messageHandler: ((message: unknown) => void) | undefined;
  let closeHandler: ((reason: string) => void) | undefined;
  let closeNotified = false;
  const pending: unknown[] = [];

  const notifyClose = (): void => {
    if (closeHandler === undefined || closeNotified) {
      return;
    }
    if (closedReason === undefined) {
      return;
    }
    closeNotified = true;
    closeHandler(closedReason);
  };

  const markClosed = (reason: string): void => {
    if (closedReason !== undefined) {
      return;
    }
    closedReason = reason;
    pending.length = 0;
    notifyClose();
  };

  process.on('message', (message: unknown) => {
    if (closedReason !== undefined) {
      return;
    }
    if (messageHandler === undefined) {
      pending.push(message);
      return;
    }
    messageHandler(message);
  });
  process.on('disconnect', () => {
    markClosed('process disconnected');
  });
  // `disconnect` does not fire when the parent dies abruptly, so `exit` is the
  // fallback that keeps a consumer waiting on `onClose` from hanging.
  process.on('exit', () => {
    markClosed('process exited');
  });

  const channel: IpcChannel = {
    send(message: IpcMessage): void {
      if (closedReason !== undefined) {
        throw new IpcChannelClosedError(closedReason);
      }
      if (!hasProcessSend) {
        throw new IpcChannelClosedError('process has no ipc channel');
      }
      let accepted: boolean | void;
      try {
        accepted = process.send?.(message);
      } catch (error) {
        throw new IpcChannelClosedError(messageOf(error));
      }
      // Bun returns a boolean and the typings say `void`; only an explicit
      // `false` means the message was dropped.
      if (accepted === false) {
        throw new IpcChannelClosedError('the ipc channel rejected the message');
      }
    },
    onMessage(handler: (message: unknown) => void): void {
      messageHandler = handler;
      const queued = pending.splice(0);
      for (const message of queued) {
        handler(message);
      }
    },
    onClose(handler: (reason: string) => void): void {
      closeHandler = handler;
      notifyClose();
    },
    close(reason?: string): void {
      if (closedReason !== undefined) {
        return;
      }
      closedReason = reason ?? DEFAULT_CLOSE_REASON;
      pending.length = 0;
      // The local side is not notified of its own close, matching the interface.
      // `disconnect` throws when the process never had an IPC channel, which is
      // not a failure: the channel is already marked closed above.
      try {
        process.disconnect?.();
      } catch {
        // Nothing to report: the peer is either gone or was never attached.
      }
    },
  };
  processChannel = channel;
  return channel;
}
