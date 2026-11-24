/**
 * Wire format between a client and the connection layer. Deliberately minimal:
 * an id for correlating the reply, a method naming a handler, and an opaque
 * payload the handler interprets. Session routing and replay sit above this
 * (SPEC 5.4), so they do not appear here.
 */
import { isRecord, messageOf } from '../internal/guards.ts';

export interface Envelope {
  id: string;
  method: string;
  payload: unknown;
}

export interface ResponseEnvelope {
  id: string;
  result?: unknown;
  error?: { code: string; message: string };
}

export interface Codec {
  encode(value: ResponseEnvelope): string | Uint8Array;
  decode(raw: string | Uint8Array): Envelope;
}

/**
 * Decoding failures carry no request id, so the connection layer has to treat
 * them as a class. A named error keeps that distinction out of message
 * matching.
 */
export class EnvelopeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EnvelopeError';
  }
}

const decoder = new TextDecoder();

export class JsonCodec implements Codec {
  encode(value: ResponseEnvelope): string {
    const body: Record<string, unknown> = { id: value.id };
    // Mutually exclusive on purpose: a consumer must never have to decide
    // whether a present result or a present error wins.
    if (value.error !== undefined) {
      body.error = value.error;
    } else {
      body.result = value.result;
    }
    return JSON.stringify(body);
  }

  decode(raw: string | Uint8Array): Envelope {
    const text = typeof raw === 'string' ? raw : decoder.decode(raw);
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch (error) {
      throw new EnvelopeError(
        `envelope is not valid JSON: ${messageOf(error)}`,
      );
    }
    if (!isRecord(parsed)) {
      throw new EnvelopeError('envelope must be a JSON object');
    }
    if (typeof parsed.id !== 'string' || parsed.id.length === 0) {
      throw new EnvelopeError('envelope "id" must be a non-empty string');
    }
    if (typeof parsed.method !== 'string' || parsed.method.length === 0) {
      throw new EnvelopeError('envelope "method" must be a non-empty string');
    }
    return { id: parsed.id, method: parsed.method, payload: parsed.payload };
  }
}
