import type {
  Transport,
  TransportConnection,
  TransportHandlers,
} from '../transport/types.ts';

interface ConnectionState {
  connection: TransportConnection;
  sent: (string | Uint8Array)[];
  open: boolean;
  /** Set while a test holds this connection's writes; see `holdSends`. */
  hold?: HoldState;
  /** Set while a test makes this connection's writes throw; see `throwOnSend`. */
  throwWith?: Error;
}

interface HoldState {
  readonly entered: () => void;
  readonly gate: Promise<void>;
}

interface CloseRecord {
  readonly code: number | undefined;
  readonly reason: string;
}

type Status = 'idle' | 'started' | 'stopped';

/**
 * A write a test keeps pending, so the connection layer can be observed while
 * `send` has not settled. The frame is recorded on entry: a real transport can
 * have the bytes on the wire while it still waits for the socket to drain.
 */
export interface HeldSends {
  /** Resolves once the held write was attempted. */
  readonly entered: Promise<void>;
  /** Records the frame and settles the pending write successfully. */
  release(): void;
  /** Fails the pending write, as a broken socket would, without recording it. */
  fail(error: Error): void;
}

/**
 * Test double for transports (SPEC 5.3): no socket, no framing. Tests drive the
 * inbound side and read back what the framework wrote.
 *
 * Handler errors are not swallowed: a synchronous throw propagates out of the
 * helper that triggered it, and an async rejection is returned by `receive` /
 * `disconnect`. The one exception is `onConnect`, whose only return channel is
 * the synchronous `connect` call — an async rejection there is dropped so it
 * cannot become an unhandled rejection.
 */
export class FakeTransport implements Transport {
  /**
   * Test hook for `Transport.sendTimeoutMs`: a declared value lets a test
   * observe which bound the layer picks for an event write.
   */
  readonly sendTimeoutMs?: number;
  private readonly connections = new Map<string, ConnectionState>();
  private readonly closes = new Map<string, CloseRecord>();
  private status: Status = 'idle';
  private handlers: TransportHandlers | undefined;
  private events: Promise<unknown> = Promise.resolve();

  constructor(options: { sendTimeoutMs?: number } = {}) {
    this.sendTimeoutMs = options.sendTimeoutMs;
  }

  start(handlers: TransportHandlers): Promise<void> {
    // Rejects rather than throws: `start`/`stop` both promise a result, so a
    // caller that only awaits must still see the failure.
    if (this.status !== 'idle') {
      return Promise.reject(
        new Error(`transport cannot start while ${this.status}`),
      );
    }
    this.handlers = handlers;
    this.status = 'started';
    return Promise.resolve();
  }

  async stop(): Promise<void> {
    if (this.status !== 'started') {
      throw new Error(`transport cannot stop while ${this.status}`);
    }
    this.status = 'stopped';
    // In-flight handler work is awaited so `stop` means no callback starts
    // afterwards. Connections are not reported as disconnected: shutdown
    // ordering belongs to the connection layer (SPEC 6.3).
    await this.events;
  }

  connect(id: string): TransportConnection {
    const state = this.register(id);
    const pending = this.invoke((handlers) =>
      handlers.onConnect(state.connection),
    );
    // `connect` returns the connection, so an async onConnect rejection has no
    // caller to reach; keep it from surfacing as an unhandled rejection.
    pending.catch(() => undefined);
    return state.connection;
  }

  receive(id: string, payload: string | Uint8Array): Promise<void> {
    const state = this.lookupOpen(id);
    return this.invoke((handlers) =>
      handlers.onMessage(state.connection, payload),
    );
  }

  disconnect(id: string, reason = 'disconnected by test'): Promise<void> {
    const state = this.lookupOpen(id);
    state.open = false;
    return this.invoke((handlers) =>
      handlers.onDisconnect(state.connection, reason),
    );
  }

  /** Snapshot of what the framework has written to this connection so far. */
  sentTo(id: string): readonly (string | Uint8Array)[] {
    this.assertStarted();
    return [...this.lookup(id).sent];
  }

  /** What the framework asked `close()` to do, if it closed this connection. */
  closedWith(id: string): CloseRecord | undefined {
    this.assertStarted();
    this.lookup(id);
    return this.closes.get(id);
  }

  /**
   * Holds the next write on `id` until the returned handle settles it; later
   * writes are unaffected. The frame is still recorded when `send` is entered,
   * so a test can distinguish "the write started" from "the write finished" —
   * the case where the layer must not treat an abort as a reason to release its
   * ordering slot.
   */
  holdSends(id: string): HeldSends {
    this.assertStarted();
    const state = this.lookup(id);
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    state.hold = { entered: () => entered.resolve(), gate: gate.promise };
    return {
      entered: entered.promise,
      release: () => gate.resolve(),
      fail: (error) => gate.reject(error),
    };
  }

  /**
   * Makes every later write on `id` throw synchronously instead of returning a
   * promise, which the interface forbids but a real implementation can do.
   */
  throwOnSend(id: string, error: Error): void {
    this.assertStarted();
    this.lookup(id).throwWith = error;
  }

  private register(id: string): ConnectionState {
    this.assertStarted();
    if (this.connections.has(id)) {
      throw new Error(`connection "${id}" already exists`);
    }
    const state: ConnectionState = {
      sent: [],
      open: true,
      connection: {
        id,
        send: (payload: string | Uint8Array): Promise<void> => {
          // Rejects rather than throws: the signature promises a result, so a
          // caller that only awaits must still observe the failure.
          if (this.status !== 'started') {
            return Promise.reject(statusError(this.status));
          }
          const current = this.connections.get(id);
          if (current === undefined || !current.open) {
            return Promise.reject(new Error(`connection "${id}" is not open`));
          }
          if (current.throwWith !== undefined) {
            // A deliberately interface-breaking transport: the layer must
            // contain a synchronous throw instead of letting it escape.
            throw current.throwWith;
          }
          current.sent.push(payload);
          const hold = current.hold;
          if (hold === undefined) {
            return Promise.resolve();
          }
          // One-shot: a later write on the same connection is not held, so a
          // test can let the follow-up event through while it inspects the
          // first one.
          current.hold = undefined;
          hold.entered();
          return hold.gate;
        },
        close: (code?: number, reason?: string): Promise<void> => {
          if (this.status !== 'started') {
            return Promise.reject(statusError(this.status));
          }
          const current = this.connections.get(id);
          if (current === undefined || !current.open) {
            return Promise.reject(new Error(`connection "${id}" is not open`));
          }
          this.closes.set(id, {
            code,
            reason: reason ?? describeClose(code),
          });
          return this.disconnect(id, reason ?? describeClose(code));
        },
      },
    };
    this.connections.set(id, state);
    return state;
  }

  private lookup(id: string): ConnectionState {
    const state = this.connections.get(id);
    if (state === undefined) {
      throw new Error(`unknown connection "${id}"`);
    }
    return state;
  }

  private lookupOpen(id: string): ConnectionState {
    // Lifecycle errors take precedence over unknown ids: a call made before
    // start is a test bug, not a missing connection.
    this.assertStarted();
    const state = this.lookup(id);
    if (!state.open) {
      throw new Error(`connection "${id}" is closed`);
    }
    return state;
  }

  private invoke(
    call: (handlers: TransportHandlers) => void | Promise<void>,
  ): Promise<void> {
    this.assertStarted();
    const handlers = this.handlers;
    if (handlers === undefined) {
      throw new Error('transport is not started');
    }
    // Invoked synchronously so a sync throw reaches the trigger site; the
    // returned promise is tracked so `stop` can wait for it.
    const settled = Promise.resolve(call(handlers));
    this.events = this.events.then(() => settled).catch(() => undefined);
    return settled;
  }

  private assertStarted(): void {
    if (this.status !== 'started') {
      throw statusError(this.status);
    }
  }
}

function statusError(status: Status): Error {
  return new Error(
    status === 'idle' ? 'transport is not started' : 'transport is stopped',
  );
}

function describeClose(code: number | undefined): string {
  return code === undefined ? 'closed' : `closed with code ${code}`;
}
