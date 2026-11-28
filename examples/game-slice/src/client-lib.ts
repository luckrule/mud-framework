import { randomUUID } from 'node:crypto';

/**
 * A reusable programmatic client for the game slice. It speaks the framework's
 * wire format directly: requests are `{id, method, payload}` and the two
 * downlink frame kinds are responses (`{id, ok, ...}`) and events
 * (`{kind:'event', eventId, event, payload}`).
 *
 * The split matters: an event frame has no response `id`, so treating every
 * frame as a response would leave requests pending forever and lose events.
 * Unknown frames are ignored (SPEC 5.4).
 */
const DEFAULT_REQUEST_TIMEOUT_MS = 5000;
const CLOSE_WAIT_MS = 1000;
const MAX_QUEUED_EVENTS = 1000;

export interface GameEvent {
  readonly eventId: string;
  readonly event: string;
  readonly payload: unknown;
}

export interface GameClientOptions {
  readonly url: string;
  /** Per-request deadline. Defaults to 5000. */
  readonly timeoutMs?: number;
}

/** A failed call: `code` is the wire code the Server returned. */
export class GameClientError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'GameClientError';
    this.code = code;
  }
}

interface PendingRequest {
  readonly resolve: (result: unknown) => void;
  readonly reject: (error: Error) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

interface EventWaiter {
  readonly predicate: (event: GameEvent) => boolean;
  readonly settle: (event: GameEvent) => void;
  readonly fail: (error: Error) => void;
}

type ParsedFrame =
  | { readonly kind: 'event'; readonly event: GameEvent }
  | {
      readonly kind: 'response';
      readonly id: string;
      readonly ok: true;
      readonly result: unknown;
    }
  | {
      readonly kind: 'response';
      readonly id: string;
      readonly ok: false;
      readonly error: { readonly code: string; readonly message: string };
    };

export class GameClient {
  private readonly socket: WebSocket;
  private readonly timeoutMs: number;
  private readonly pending = new Map<string, PendingRequest>();
  private readonly listeners = new Set<(event: GameEvent) => void>();
  private readonly waiters = new Set<EventWaiter>();
  private readonly eventQueue: GameEvent[] = [];
  private readonly closed = Promise.withResolvers<void>();
  private isClosed = false;

  private constructor(socket: WebSocket, timeoutMs: number) {
    this.socket = socket;
    this.timeoutMs = timeoutMs;
    socket.addEventListener('message', (event: MessageEvent) => {
      const raw: unknown = event.data;
      this.handleFrame(raw);
    });
    socket.addEventListener('close', () => {
      this.failAll(new GameClientError('CLIENT_CLOSED', 'connection closed'));
    });
    socket.addEventListener('error', () => {
      this.failAll(new GameClientError('CLIENT_CLOSED', 'websocket error'));
    });
  }

  static connect(options: GameClientOptions): Promise<GameClient> {
    const timeoutMs = options.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    assertTimeout(timeoutMs);
    return new Promise<GameClient>((resolve, reject) => {
      const socket = new WebSocket(options.url);
      let timer: ReturnType<typeof setTimeout> | undefined;
      let settled = false;

      const finish = (): boolean => {
        if (settled) {
          return false;
        }
        settled = true;
        if (timer !== undefined) {
          clearTimeout(timer);
        }
        socket.removeEventListener('open', onOpen);
        socket.removeEventListener('error', onError);
        socket.removeEventListener('close', onClose);
        return true;
      };
      const onOpen = (): void => {
        if (finish()) {
          resolve(new GameClient(socket, timeoutMs));
        }
      };
      const onError = (): void => {
        if (finish()) {
          reject(new Error(`could not connect to ${options.url}`));
        }
      };
      const onClose = (): void => {
        if (finish()) {
          reject(
            new Error(`connection to ${options.url} closed before opening`),
          );
        }
      };

      socket.addEventListener('open', onOpen);
      socket.addEventListener('error', onError);
      socket.addEventListener('close', onClose);
      timer = setTimeout(() => {
        if (finish()) {
          socket.close();
          reject(
            new Error(
              `could not connect to ${options.url} within ${timeoutMs}ms`,
            ),
          );
        }
      }, timeoutMs);
    });
  }

  /**
   * Sends one request and resolves with its result.
   *
   * `clientRequestId` is the wire correlation id; a caller that retries the
   * same logical command passes the same id so the retry is recognisable as a
   * repeat (logs, duplicate statistics) instead of a new command. Omitted, a
   * fresh id is minted per call.
   */
  call<T = unknown>(
    method: string,
    payload?: unknown,
    clientRequestId?: string,
  ): Promise<T> {
    if (this.isClosed) {
      return Promise.reject(
        new GameClientError('CLIENT_CLOSED', 'client is closed'),
      );
    }
    const id = clientRequestId ?? randomUUID();
    if (id.length === 0) {
      return Promise.reject(
        new GameClientError('CLIENT_BAD_ID', 'request id must not be empty'),
      );
    }
    if (this.pending.has(id)) {
      return Promise.reject(
        new GameClientError(
          'CLIENT_DUPLICATE_ID',
          `request ${id} is already in flight`,
        ),
      );
    }
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(
          new GameClientError(
            'CLIENT_TIMEOUT',
            `${method} timed out after ${this.timeoutMs}ms`,
          ),
        );
      }, this.timeoutMs);
      this.pending.set(id, {
        resolve: (result) => {
          // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- the wire result is opaque; the caller names the expected shape
          resolve(result as T);
        },
        reject,
        timer,
      });
      try {
        this.socket.send(JSON.stringify({ id, method, payload }));
      } catch (error) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(new GameClientError('CLIENT_SEND_FAILED', messageOf(error)));
      }
    });
  }

  /** Observes every event; returns an unsubscribe function. */
  onEvent(listener: (event: GameEvent) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /**
   * Resolves with the next event matching `predicate`, including one that was
   * already queued. The queue has a cap, so a client that never calls this and
   * has no listener drops the oldest events instead of growing without bound.
   */
  nextEvent(
    predicate: (event: GameEvent) => boolean = () => true,
    timeoutMs: number = this.timeoutMs,
  ): Promise<GameEvent> {
    assertTimeout(timeoutMs);
    const index = this.eventQueue.findIndex((event) => predicate(event));
    if (index !== -1) {
      const [queued] = this.eventQueue.splice(index, 1);
      if (queued !== undefined) {
        return Promise.resolve(queued);
      }
    }
    if (this.isClosed) {
      return Promise.reject(
        new GameClientError('CLIENT_CLOSED', 'client is closed'),
      );
    }
    return new Promise<GameEvent>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters.delete(waiter);
        reject(
          new GameClientError(
            'CLIENT_TIMEOUT',
            `no matching event within ${timeoutMs}ms`,
          ),
        );
      }, timeoutMs);
      const waiter: EventWaiter = {
        predicate,
        settle: (event) => {
          clearTimeout(timer);
          this.waiters.delete(waiter);
          resolve(event);
        },
        fail: (error) => {
          clearTimeout(timer);
          this.waiters.delete(waiter);
          reject(error);
        },
      };
      this.waiters.add(waiter);
    });
  }

  /**
   * Closes the socket and settles every pending call and waiter. The wait for
   * the socket's `close` event is bounded: a connecting socket may never emit
   * one.
   */
  async close(): Promise<void> {
    this.failAll(new GameClientError('CLIENT_CLOSED', 'client closed'));
    if (
      this.socket.readyState === WebSocket.OPEN ||
      this.socket.readyState === WebSocket.CONNECTING
    ) {
      this.socket.close(1000, 'client closed');
    }
    await Promise.race([this.closed.promise, Bun.sleep(CLOSE_WAIT_MS)]);
  }

  private handleFrame(raw: unknown): void {
    if (typeof raw !== 'string') {
      return;
    }
    const frame = parseFrame(raw);
    if (frame === undefined) {
      return;
    }
    if (frame.kind === 'event') {
      this.deliverEvent(frame.event);
      return;
    }
    const pending = this.pending.get(frame.id);
    if (pending === undefined) {
      // A response for a call that already timed out, or an unknown id.
      return;
    }
    this.pending.delete(frame.id);
    clearTimeout(pending.timer);
    if (frame.ok) {
      pending.resolve(frame.result);
      return;
    }
    pending.reject(new GameClientError(frame.error.code, frame.error.message));
  }

  private deliverEvent(event: GameEvent): void {
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch {
        // One broken observer must not stop delivery to the rest.
      }
    }
    for (const waiter of this.waiters) {
      let matched: boolean;
      try {
        matched = waiter.predicate(event);
      } catch {
        matched = false;
      }
      if (!matched) {
        continue;
      }
      waiter.settle(event);
      return;
    }
    this.eventQueue.push(event);
    if (this.eventQueue.length > MAX_QUEUED_EVENTS) {
      this.eventQueue.shift();
    }
  }

  private failAll(error: Error): void {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
    for (const waiter of this.waiters) {
      waiter.fail(error);
    }
    this.waiters.clear();
    this.closed.resolve();
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseFrame(raw: string): ParsedFrame | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return undefined;
  }
  if (!isRecord(parsed)) {
    return undefined;
  }
  if (parsed.kind === 'event') {
    const { eventId, event, payload } = parsed;
    if (typeof eventId !== 'string' || typeof event !== 'string') {
      return undefined;
    }
    if (eventId.length === 0 || event.length === 0) {
      return undefined;
    }
    return { kind: 'event', event: { eventId, event, payload } };
  }
  const { id, ok } = parsed;
  if (typeof id !== 'string' || typeof ok !== 'boolean') {
    return undefined;
  }
  if (ok) {
    return { kind: 'response', id, ok: true, result: parsed.result };
  }
  const error = parsed.error;
  if (
    !isRecord(error) ||
    typeof error.code !== 'string' ||
    typeof error.message !== 'string'
  ) {
    return undefined;
  }
  return {
    kind: 'response',
    id,
    ok: false,
    error: { code: error.code, message: error.message },
  };
}

function assertTimeout(value: number): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(
      `timeoutMs must be a positive finite number, got ${value}`,
    );
  }
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
