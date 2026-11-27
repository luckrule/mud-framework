import type { Logger } from '../logging/types.ts';
import type { ResponseEnvelope } from '../shared/codec.ts';
import { messageOf } from '../shared/guards.ts';
import {
  INTERNAL_ERROR,
  INVALID_ENVELOPE,
  TRANSPORT_SEND_FAILED,
} from '../shared/wire-codes.ts';
import type { TransportConnection } from '../transport/types.ts';
import type { LayerHost } from './connection-host.ts';

/** The wire format and the sink for failures that must not reach the transport. */
export type ReplyHost = Pick<LayerHost, 'codec' | 'logger'>;

/** Length only: the raw frame may hold client data that must not be logged. */
export function rawLengthOf(payload: string | Uint8Array): number {
  return typeof payload === 'string' ? payload.length : payload.byteLength;
}

/**
 * Sends one frame. A result JSON cannot represent (cycle, BigInt) must still
 * produce an answer rather than a stuck request; a write to a closed socket must
 * be recorded instead of thrown, because the transport has no caller to hand it
 * to and no way to answer a request nobody can read.
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
        { code: INTERNAL_ERROR, reason: messageOf(error) },
        'response could not be encoded',
      );
      frame = host.codec.encode({
        id: response.id,
        ok: false,
        error: {
          code: INTERNAL_ERROR,
          message: 'result could not be encoded',
        },
      });
    }
    await connection.send(frame);
  } catch (error) {
    logger.error(
      { code: TRANSPORT_SEND_FAILED, reason: messageOf(error) },
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
