import type { IpcMessage } from './protocol.ts';

/**
 * Transport-agnostic view of the Portal <-> Server link (ADR-0002).
 *
 * Deliberately free of `Bun.*` types: the real implementation wraps
 * `Bun.spawn({ ipc })` and the in-memory pair below serves tests, and callers
 * must be able to swap one for the other. Inbound payloads are `unknown`
 * because the peer can send anything (corrupt or out-of-protocol data);
 * consumers narrow them with `parseIpcMessage` (protocol.ts) and must not
 * assume a shape.
 */
export interface IpcChannel {
  /**
   * Sends one message. Throws `IpcChannelClosedError` when either side is
   * closed: a dropped message would leave the sender waiting for a result that
   * can never arrive, so the failure has to be observable.
   */
  send(message: IpcMessage): void;
  /**
   * Registers the inbound handler, replacing any previous one. Messages that
   * arrived before registration are delivered, in order, at registration time
   * rather than discarded.
   */
  onMessage(handler: (message: unknown) => void): void;
  /**
   * Registers the close handler. Fires at most once. If the channel is already
   * closed, the handler is invoked immediately with the recorded reason, so a
   * close that beats subscription is never lost.
   */
  onClose(handler: (reason: string) => void): void;
  /**
   * Closes this channel and notifies the peer. Idempotent: closing an already
   * closed channel does nothing. The local side does not receive its own
   * `onClose`; it already knows.
   */
  close(reason?: string): void;
}

export class IpcChannelClosedError extends Error {
  constructor(reason: string) {
    super(`ipc channel is closed: ${reason}`);
    this.name = 'IpcChannelClosedError';
  }
}

export type InMemoryChannelPair = readonly [IpcChannel, IpcChannel];

interface Endpoint {
  onMessage: ((message: unknown) => void) | undefined;
  onClose: ((reason: string) => void) | undefined;
  /** Messages that arrived before a handler was registered. */
  pending: unknown[];
  /** Set once the pair closed; the reason reported to this endpoint. */
  closedReason: string | undefined;
  /** True when this endpoint itself called `close`. */
  closedLocally: boolean;
  /** Guards the at-most-once contract of `onClose`. */
  notified: boolean;
}

export const DEFAULT_CLOSE_REASON = 'closed by local side';

/**
 * Two connected channels for unit tests, with no process and no serialization.
 *
 * Delivery is synchronous, so a test never has to poll for a message. That is
 * the one behavioural difference from `Bun.spawn` IPC, which delivers on the
 * event loop; tests should therefore not depend on ordering relative to
 * unrelated async work. A handler that throws propagates the throw to the
 * sender (the real channel cannot: Bun logs and drops it), which keeps a
 * broken handler visible instead of silently passing.
 *
 * Close is a property of the pair, not of one endpoint: there is no half-open
 * state, because a dead Server process takes the link with it. The closing side
 * is not notified; the peer is, exactly once.
 */
export function createInMemoryChannelPair(): InMemoryChannelPair {
  const states: [Endpoint, Endpoint] = [newEndpoint(), newEndpoint()];

  function deliver(target: Endpoint, message: unknown): void {
    if (target.closedReason !== undefined) {
      return;
    }
    const handler = target.onMessage;
    if (handler === undefined) {
      target.pending.push(message);
      return;
    }
    handler(message);
  }

  function notifyClose(target: Endpoint): void {
    const reason = target.closedReason;
    if (reason === undefined || target.notified || target.closedLocally) {
      return;
    }
    const handler = target.onClose;
    if (handler === undefined) {
      // Leave `notified` false: a close that beats subscription must still
      // reach the handler registered later (see `onClose` in the interface).
      return;
    }
    target.notified = true;
    handler(reason);
  }

  function closeFrom(index: 0 | 1, reason: string | undefined): void {
    const self = states[index];
    const peer = states[1 - index];
    if (self.closedReason !== undefined) {
      return;
    }
    const resolved = reason ?? DEFAULT_CLOSE_REASON;
    self.closedReason = resolved;
    self.closedLocally = true;
    self.pending = [];
    peer.closedReason = resolved;
    peer.pending = [];
    notifyClose(peer);
  }

  function createChannel(index: 0 | 1): IpcChannel {
    const self = states[index];
    const peer = states[1 - index];
    return {
      send(message: IpcMessage): void {
        if (self.closedReason !== undefined) {
          throw new IpcChannelClosedError(self.closedReason);
        }
        if (peer.closedReason !== undefined) {
          throw new IpcChannelClosedError(peer.closedReason);
        }
        deliver(peer, message);
      },
      onMessage(handler: (message: unknown) => void): void {
        self.onMessage = handler;
        // Dequeue one at a time and only after delivery: a handler that throws
        // must leave the rest queued for the next registration rather than
        // losing them, which is what the interface promises. A handler that
        // closes during replay stops delivery to a closed endpoint.
        while (self.pending.length > 0 && self.closedReason === undefined) {
          const message = self.pending[0];
          handler(message);
          self.pending.shift();
        }
      },
      onClose(handler: (reason: string) => void): void {
        self.onClose = handler;
        notifyClose(self);
      },
      close(reason?: string): void {
        closeFrom(index, reason);
      },
    };
  }

  return [createChannel(0), createChannel(1)];
}

function newEndpoint(): Endpoint {
  return {
    onMessage: undefined,
    onClose: undefined,
    pending: [],
    closedReason: undefined,
    closedLocally: false,
    notified: false,
  };
}
