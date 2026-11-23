import { randomUUID } from 'node:crypto';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type {
  Transport,
  TransportConnection,
  TransportHandlers,
} from '../transport/types.ts';
import { JsonCodec } from './codec.ts';
import type { Codec, ResponseEnvelope } from './codec.ts';
import { FrameworkTimeoutError, UnknownHandlerError } from './dispatcher.ts';
import type { Dispatcher } from './dispatcher.ts';

export interface ConnectionLayerOptions {
  transport: Transport;
  dispatcher: Dispatcher;
  /** Defaults to `JsonCodec`; swapping it swaps the wire format. */
  codec?: Codec;
  /**
   * Defaults to the JSON logger. Failures that must not reach the transport (a
   * write to a closed socket, a result that cannot be encoded) are recorded
   * here rather than dropped.
   */
  logger?: Logger;
  /**
   * Defaults to `randomUUID`. The id is minted here, not in the dispatcher,
   * because a request enters the framework at this boundary and every log line
   * about it — including a dispatch failure — must carry the same id (SPEC 7.3).
   */
  createRequestId?: () => string;
}

type Status = 'idle' | 'started' | 'stopped';

interface ConnectionState {
  readonly connection: TransportConnection;
  /** Tail of this connection's work chain; see `onMessage`. */
  chain: Promise<unknown>;
}

/**
 * Wires a transport to the dispatcher. It owns only what sits between the two:
 * decode, per-connection ordering, error mapping and encode (SPEC 5.4). Session
 * routing, replay and input buffering are deliberately absent.
 */
export class ConnectionLayer implements TransportHandlers {
  private readonly transport: Transport;
  private readonly dispatcher: Dispatcher;
  private readonly codec: Codec;
  private readonly logger: Logger;
  private readonly createRequestId: () => string;
  private readonly connections = new Map<string, ConnectionState>();
  private status: Status = 'idle';

  constructor(options: ConnectionLayerOptions) {
    this.transport = options.transport;
    this.dispatcher = options.dispatcher;
    this.codec = options.codec ?? new JsonCodec();
    this.logger =
      options.logger ??
      createJsonLogger({ bindings: { component: 'connection-layer' } });
    this.createRequestId = options.createRequestId ?? randomUUID;
  }

  async start(): Promise<void> {
    if (this.status !== 'idle') {
      throw new Error(`connection layer cannot start while ${this.status}`);
    }
    await this.transport.start(this);
    this.status = 'started';
  }

  onConnect(connection: TransportConnection): Promise<void> {
    this.connections.set(connection.id, {
      connection,
      chain: Promise.resolve(),
    });
    return Promise.resolve();
  }

  onMessage(
    connection: TransportConnection,
    payload: string | Uint8Array,
  ): Promise<void> {
    const state = this.connections.get(connection.id);
    if (state === undefined) {
      // Not a tracked connection (a race with stop, or a transport that
      // reconnects with a fresh object): there is nobody to answer.
      return Promise.resolve();
    }
    // Per-connection chain, not a global one: requests on one connection must
    // observe their predecessor's writes (SPEC 3.1), while different
    // connections must stay concurrent (SPEC 3.2). Awaiting the previous tail
    // also makes a request that timed out release the next one immediately,
    // because the dispatcher settles at the timeout, not at handler return.
    const next = state.chain.then(() => this.handle(connection, payload));
    // The chain must survive a rejected link, otherwise one failure would
    // poison every later request on this connection.
    state.chain = next.catch(() => undefined);
    return next;
  }

  onDisconnect(connection: TransportConnection): Promise<void> {
    this.connections.delete(connection.id);
    return Promise.resolve();
  }

  async stop(): Promise<void> {
    if (this.status !== 'started') {
      return;
    }
    this.status = 'stopped';
    // Transport first: it stops accepting input and settles in-flight handler
    // promises, so nothing new arrives while the backend is being closed.
    await this.transport.stop();
    this.connections.clear();
    await this.dispatcher.close();
  }

  private async handle(
    connection: TransportConnection,
    payload: string | Uint8Array,
  ): Promise<void> {
    let method: string;
    let clientRequestId: string;
    let requestPayload: unknown;
    try {
      const envelope = this.codec.decode(payload);
      method = envelope.method;
      clientRequestId = envelope.id;
      requestPayload = envelope.payload;
    } catch (error) {
      // warn, not error: one unreadable frame is a rejected input, not a
      // failed request (SPEC 7.1).
      this.logger.warn(
        {
          connectionId: connection.id,
          code: 'INVALID_ENVELOPE',
          rawLength: rawLengthOf(payload),
          reason: messageOf(error),
        },
        'envelope rejected',
      );
      // The id is unreadable, so the reply cannot be correlated. Answering
      // with an empty id beats closing the connection: a client sees a stable
      // error code it can log, and one bad frame does not destroy a session
      // that may otherwise be healthy (SPEC 3.5 forbids silent drops).
      await this.reply(connection, {
        id: '',
        error: { code: 'INVALID_ENVELOPE', message: messageOf(error) },
      });
      return;
    }

    // Assigned here, at the framework's entry point, before any client data
    // influences it (SPEC 7.3). The dispatcher honours it rather than minting
    // its own, so both layers log the same id; a replay passes it back in.
    const requestId = this.createRequestId();
    const logger = this.logger.child({
      requestId,
      connectionId: connection.id,
      clientRequestId,
    });
    const startedAt = performance.now();

    let response: ResponseEnvelope;
    try {
      const result = await this.dispatcher.dispatch(method, requestPayload, {
        requestId,
        connectionId: connection.id,
        clientRequestId,
      });
      response = { id: clientRequestId, result };
    } catch (error) {
      const described = describeError(error);
      logger.error(
        {
          method,
          code: described.code,
          durationMs: elapsedSince(startedAt),
          reason: described.message,
        },
        'request failed',
      );
      response = { id: clientRequestId, error: described };
    }
    await this.reply(connection, response, logger);
  }

  private async reply(
    connection: TransportConnection,
    response: ResponseEnvelope,
    logger: Logger = this.logger,
  ): Promise<void> {
    try {
      let frame: string | Uint8Array;
      try {
        frame = this.codec.encode(response);
      } catch (error) {
        // A handler result JSON cannot represent (cycle, BigInt) must still
        // produce an answer rather than a stuck request.
        logger.error(
          { code: 'INTERNAL_ERROR', reason: messageOf(error) },
          'response could not be encoded',
        );
        frame = this.codec.encode({
          id: response.id,
          error: {
            code: 'INTERNAL_ERROR',
            message: 'result could not be encoded',
          },
        });
      }
      await connection.send(frame);
    } catch (error) {
      // The socket may have closed between dispatch and write. Rethrowing
      // would surface as an unhandled rejection in the transport, which has no
      // caller to hand it to and no way to answer a request nobody can read.
      logger.error(
        { code: 'TRANSPORT_SEND_FAILED', reason: messageOf(error) },
        'response could not be sent',
      );
    }
  }
}

function describeError(error: unknown): { code: string; message: string } {
  if (error instanceof FrameworkTimeoutError) {
    return { code: error.code, message: error.message };
  }
  if (error instanceof UnknownHandlerError) {
    return { code: error.code, message: error.message };
  }
  return { code: 'INTERNAL_ERROR', message: messageOf(error) };
}

/** Length only: the raw frame may hold client data that must not be logged. */
function rawLengthOf(payload: string | Uint8Array): number {
  return typeof payload === 'string' ? payload.length : payload.byteLength;
}

function elapsedSince(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
