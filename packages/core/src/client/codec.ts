/**
 * Wire format between a client and the connection layer. Deliberately minimal:
 * an id for correlating the reply, a method naming a handler, and an opaque
 * payload the handler interprets. Session routing and replay sit above this
 * (SPEC 5.2), so they do not appear here.
 */
import { isRecord, messageOf } from '../shared/guards.ts';

export interface Envelope {
  id: string;
  method: string;
  payload: unknown;
}

/**
 * A discriminated union rather than two optional fields. With optionals, `{}`
 * and `{result, error}` are both representable, so every consumer would have to
 * decide which field wins; and `JSON.stringify` drops an `undefined` result, so
 * a void handler's success is indistinguishable from a frame with no body at
 * all. The `ok` flag is on the wire for exactly that reason: `{"id":"x","ok":true}`
 * says "succeeded, no value", which no optional-field shape can express.
 *
 * This matches how `ExecutorOutcome` and `ResultMessage` already discriminate.
 */
export type ResponseEnvelope = { id: string } & (
  | { ok: true; result: unknown }
  | { ok: false; error: { code: string; message: string } }
);

export interface Codec {
  encode(value: ResponseEnvelope): string | Uint8Array;
  decode(raw: string | Uint8Array): Envelope;
}

const decoder = new TextDecoder();

export class JsonCodec implements Codec {
  encode(value: ResponseEnvelope): string {
    // Spelled out per branch rather than spreading the rest of the object: the
    // wire shape must be readable here, and `ok` must never be joined by the
    // other branch's field.
    return JSON.stringify(
      value.ok
        ? { id: value.id, ok: true, result: value.result }
        : { id: value.id, ok: false, error: value.error },
    );
  }

  decode(raw: string | Uint8Array): Envelope {
    const text = typeof raw === 'string' ? raw : decoder.decode(raw);
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch (error) {
      throw new Error(`envelope is not valid JSON: ${messageOf(error)}`, {
        cause: error,
      });
    }
    if (!isRecord(parsed)) {
      throw new Error('envelope must be a JSON object');
    }
    if (typeof parsed.id !== 'string' || parsed.id.length === 0) {
      throw new Error('envelope "id" must be a non-empty string');
    }
    if (typeof parsed.method !== 'string' || parsed.method.length === 0) {
      throw new Error('envelope "method" must be a non-empty string');
    }
    return { id: parsed.id, method: parsed.method, payload: parsed.payload };
  }
}
