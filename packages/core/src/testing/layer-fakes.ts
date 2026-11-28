import { Buffer } from 'node:buffer';

import { isRecord } from 'mud-framework-test-support';

import type { Codec, Envelope, ServerFrame } from '../shared/codec.ts';
import type { RequestExecutor } from '../shared/executor.ts';
import type { Transport, TransportConnection } from '../transport/types.ts';

/** Base64 frames: proves the codec is the only wire-format decision point. */
export class Base64Codec implements Codec {
  encode(value: ServerFrame): string {
    return Buffer.from(JSON.stringify(value), 'utf8').toString('base64');
  }

  decode(raw: string | Uint8Array): Envelope {
    const text =
      typeof raw === 'string' ? raw : Buffer.from(raw).toString('utf8');
    const parsed: unknown = JSON.parse(
      Buffer.from(text, 'base64').toString('utf8'),
    );
    if (
      !isRecord(parsed) ||
      typeof parsed.id !== 'string' ||
      typeof parsed.method !== 'string'
    ) {
      throw new Error('malformed base64 envelope');
    }
    return { id: parsed.id, method: parsed.method, payload: parsed.payload };
  }
}

export interface ScriptedExecutor {
  executor: RequestExecutor;
  order: string[];
  closeCalls: () => number;
}

/** Records start/close calls so shutdown ordering can be asserted. */
export function scriptedExecutor(order: string[]): ScriptedExecutor {
  let closeCalls = 0;
  return {
    order,
    closeCalls: () => closeCalls,
    executor: {
      start: () => {
        order.push('executor.start');
        return Promise.resolve();
      },
      execute: () => Promise.resolve({ ok: true, result: null }),
      onEvent: () => () => undefined,
      cancel: () => Promise.resolve(),
      close: () => {
        closeCalls += 1;
        order.push('executor.close');
        return Promise.resolve();
      },
      health: () => ({ state: 'ready', ready: true }),
    },
  };
}

export interface ScriptedTransport {
  transport: Transport;
  stopCalls: () => number;
  failStop: (error: Error) => void;
}

/** Records start/stop calls, and can be told to make `stop` reject. */
export function scriptedTransport(order: string[]): ScriptedTransport {
  let stopCalls = 0;
  let stopFailure: Error | undefined;
  return {
    transport: {
      start: () => {
        order.push('transport.start');
        return Promise.resolve();
      },
      stop: () => {
        stopCalls += 1;
        order.push('transport.stop');
        return stopFailure === undefined
          ? Promise.resolve()
          : Promise.reject(stopFailure);
      },
    },
    stopCalls: () => stopCalls,
    failStop: (error) => {
      stopFailure = error;
    },
  };
}

/** A connection that records frames, so a test can read the reply. */
export function fakeConnection(id: string): {
  connection: TransportConnection;
  sent: string[];
} {
  const sent: string[] = [];
  return {
    sent,
    connection: {
      id,
      send: (payload) => {
        sent.push(typeof payload === 'string' ? payload : '<binary>');
        return Promise.resolve();
      },
      close: () => Promise.resolve(),
    },
  };
}
