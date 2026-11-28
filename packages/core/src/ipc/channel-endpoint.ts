import type { IpcChannel } from './channel.ts';
import { IpcChannelClosedError } from './channel.ts';
import type { IpcMessage } from './protocol.ts';

export interface EndpointChannelOptions {
  /** Delivers one message on the open link; throws `IpcChannelClosedError` on failure. */
  send: (message: IpcMessage) => void;
  /** Tears down the local end. Already disconnected is not a failure. */
  disconnect: () => void;
  /**
   * True when a close issued by this channel must never reach its own close
   * handler, not even one registered afterwards. False lets that later handler
   * observe the recorded reason, which is what `createProcessChannel` promises.
   */
  suppressNotifyOnLocalClose: boolean;
  /** Recorded when `close()` is called without one. */
  defaultCloseReason: string;
  /** Runs once when this side closes, before `disconnect`. */
  onLocalClose?: () => void;
}

/**
 * The parts one endpoint of an IPC link needs beyond `IpcChannel`: the peer
 * events that feed it and the recorded close state its own exit path reads.
 */
export interface EndpointChannel extends IpcChannel {
  /** One raw inbound message; queued until `onMessage` registers a handler. */
  receive(message: unknown): void;
  /** The peer closed the link; reported through `onClose` at most once. */
  markClosed(reason: string): void;
  /** The reason the link is closed, or `undefined` while it is open. */
  readonly closedReason: string | undefined;
}

/**
 * One end of a process IPC link: close-at-most-once, inbound messages queued
 * until a handler registers, and a local close that is not reported back to the
 * closing side. The two Bun endpoints differ only in how they send and
 * disconnect and in whether a handler registered after a local close may still
 * observe it, so those are options rather than copies.
 */
export function createEndpointChannel(
  options: EndpointChannelOptions,
): EndpointChannel {
  let closedReason: string | undefined;
  let messageHandler: ((message: unknown) => void) | undefined;
  let closeHandler: ((reason: string) => void) | undefined;
  let closeNotified = false;
  let locallyClosed = false;
  const pending: unknown[] = [];

  function notifyClose(): void {
    if (closeHandler === undefined || closeNotified) {
      return;
    }
    if (closedReason === undefined) {
      return;
    }
    if (locallyClosed && options.suppressNotifyOnLocalClose) {
      return;
    }
    closeNotified = true;
    closeHandler(closedReason);
  }

  return {
    get closedReason() {
      return closedReason;
    },
    send(message: IpcMessage): void {
      if (closedReason !== undefined) {
        throw new IpcChannelClosedError(closedReason);
      }
      options.send(message);
    },
    receive(message: unknown): void {
      if (closedReason !== undefined) {
        return;
      }
      if (messageHandler === undefined) {
        pending.push(message);
        return;
      }
      messageHandler(message);
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
    markClosed(reason: string): void {
      if (closedReason !== undefined) {
        return;
      }
      closedReason = reason;
      pending.length = 0;
      notifyClose();
    },
    close(reason?: string): void {
      if (closedReason !== undefined) {
        return;
      }
      locallyClosed = true;
      options.onLocalClose?.();
      closedReason = reason ?? options.defaultCloseReason;
      pending.length = 0;
      try {
        options.disconnect();
      } catch {
        // The peer is either gone or was never attached; the channel is
        // already marked closed, which is the state the caller wanted.
      }
    },
  };
}
