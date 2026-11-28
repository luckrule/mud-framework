import type { Logger } from '../logging/types.ts';
import type { ResponseEnvelope } from '../shared/codec.ts';
import { settleWithin } from '../shared/timeout.ts';
import { messageOf } from '../shared/validation.ts';
import {
  INTERNAL_ERROR,
  INVALID_ENVELOPE,
  TRANSPORT_SEND_FAILED,
} from '../shared/wire-codes.ts';
import type { TransportConnection } from '../transport/types.ts';
import type { LayerHost } from './connection-host.ts';

/**
 * The wire format, the sink for failures that must not reach the transport, and
 * the write bound shared with event delivery.
 */
export type ReplyHost = Pick<LayerHost, 'codec' | 'logger' | 'sendTimeoutMs'>;

/** Length only: the raw frame may hold client data that must not be logged. */
export function rawLengthOf(payload: string | Uint8Array): number {
  return typeof payload === 'string' ? payload.length : payload.byteLength;
}

/**
 * Sends one frame. A result JSON cannot represent (cycle, BigInt) must still
 * produce an answer rather than a stuck request; a write to a closed socket must
 * be recorded instead of thrown, because the transport has no caller to hand it
 * to and no way to answer a request nobody can read.
 *
 * The write is bounded by `host.sendTimeoutMs`, the same bound event delivery
 * uses (SPEC 3.8). Without it a frame the client never reads would hold the
 * connection's single writer forever, and because the reply is part of the
 * request work, the Session's terminal barrier with it — other connections
 * waiting on that Session would never continue. A timeout is recorded and the
 * caller carries on: SPEC 5.4 promises no response for an unwritable frame, and
 * SPEC 3.8 keeps the request's success unchanged, so a failure here must not
 * become an error the request does not have.
 *
 * Failures are warn, not error, for that same reason: an unwritable frame is a
 * degraded channel, not a failed request, and SPEC 7.1 reserves error for
 * request failures. `stage: 'encode'` covers both the frame and the fallback
 * that replaces an unencodable result; only `connection.send` failing is
 * `stage: 'send'`. `connectionId` names the connection whose write was lost — a
 * refusal answered here never went through the request scheduler, so no other
 * line carries it.
 */
export async function reply(
  host: ReplyHost,
  connection: TransportConnection,
  response: ResponseEnvelope,
  logger: Logger = host.logger,
): Promise<void> {
  try {
    let frame: string | Uint8Array;
    try {
      frame = host.codec.encode(response);
    } catch (error) {
      logger.error(
        {
          connectionId: connection.id,
          code: INTERNAL_ERROR,
          stage: 'encode',
          reason: messageOf(error),
        },
        'response could not be encoded',
      );
      try {
        frame = host.codec.encode({
          id: response.id,
          ok: false,
          error: {
            code: INTERNAL_ERROR,
            message: 'result could not be encoded',
          },
        });
      } catch (fallbackError) {
        // The fallback failed at the same stage as the frame it replaces;
        // reporting it as a send failure would name the wrong stage.
        logger.warn(
          {
            connectionId: connection.id,
            code: TRANSPORT_SEND_FAILED,
            stage: 'encode',
            reason: messageOf(fallbackError),
          },
          'response could not be sent',
        );
        return;
      }
    }
    // `settleWithin` consumes a rejection, so a late failure cannot escape as
    // an unhandled rejection; a synchronous throw still reaches the catch below.
    const written = await settleWithin(
      connection.send(frame),
      host.sendTimeoutMs,
    );
    if (written.status === 'timeout') {
      logger.warn(
        {
          connectionId: connection.id,
          code: TRANSPORT_SEND_FAILED,
          stage: 'send',
          reason: `the transport did not settle the write within ${host.sendTimeoutMs}ms`,
        },
        'response could not be sent',
      );
    } else if (written.status === 'rejected') {
      logger.warn(
        {
          connectionId: connection.id,
          code: TRANSPORT_SEND_FAILED,
          stage: 'send',
          reason: messageOf(written.error),
        },
        'response could not be sent',
      );
    }
  } catch (error) {
    logger.warn(
      {
        connectionId: connection.id,
        code: TRANSPORT_SEND_FAILED,
        stage: 'send',
        reason: messageOf(error),
      },
      'response could not be sent',
    );
  }
}

/**
 * Answers an unreadable frame with `INVALID_ENVELOPE`. warn, not error: one bad
 * frame is a rejected input, not a failed request (SPEC 7.1). The id is
 * unreadable, so the reply cannot be correlated; an empty id still beats closing
 * a connection that may otherwise be healthy (SPEC 3.5 forbids silent drops).
 */
export async function rejectEnvelope(
  host: ReplyHost,
  connection: TransportConnection,
  rawLength: number,
  reason: string,
): Promise<void> {
  host.logger.warn(
    {
      connectionId: connection.id,
      code: INVALID_ENVELOPE,
      rawLength,
      reason,
    },
    'envelope rejected',
  );
  await reply(host, connection, {
    id: '',
    ok: false,
    error: { code: INVALID_ENVELOPE, message: reason },
  });
}
