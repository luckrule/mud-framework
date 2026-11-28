import { expect } from 'bun:test';

import type { IpcMessage } from './protocol.ts';
import { parseIpcMessage } from './protocol.ts';

export const capabilities = {
  transactions: 'full',
  concurrency: 'pooled',
  snapshotRead: true,
  uniqueConstraint: true,
  maxTransactionDurationMs: 5000,
} as const;

export const validMessages: IpcMessage[] = [
  { type: 'hello' },
  {
    type: 'request',
    requestId: 'req-1',
    connectionId: 'conn-1',
    clientRequestId: 'client-1',
    sessionId: 'session-1',
    method: 'look',
    payload: { target: 'rock' },
    timeoutMs: 1000,
    replayCount: 0,
  },
  {
    type: 'request',
    requestId: 'req-2',
    connectionId: 'conn-1',
    method: 'look',
    payload: null,
    timeoutMs: 1000,
    replayCount: 1,
  },
  { type: 'cancel', requestId: 'req-1', code: 'FRAMEWORK_TIMEOUT' },
  { type: 'shutdown', timeoutMs: 5000 },
  { type: 'prepared', pid: 4242 },
  { type: 'activate' },
  { type: 'ready', pid: 4242, capabilities },
  { type: 'result', requestId: 'req-1', ok: true, result: { hp: 3 } },
  {
    type: 'result',
    requestId: 'req-1',
    ok: true,
    result: 'welcome',
    sessionChange: { type: 'bind', sessionId: 'session-1' },
  },
  {
    type: 'result',
    requestId: 'req-1',
    ok: true,
    result: 'bye',
    sessionChange: { type: 'unbind' },
  },
  {
    type: 'result',
    requestId: 'req-1',
    ok: false,
    error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
  },
  {
    type: 'event',
    eventId: 'req-1:0',
    target: { kind: 'connection', connectionId: 'conn-1' },
    event: 'room.description',
    payload: { text: 'a cave' },
  },
  { type: 'closed', reason: 'shutdown complete' },
];

export function rejectionReason(raw: unknown): string {
  const parsed = parseIpcMessage(raw);
  if (parsed.ok) {
    throw new Error(
      `expected rejection, got ${JSON.stringify(parsed.message)}`,
    );
  }
  return parsed.reason;
}

export function expectRejected(raw: unknown, fragment: string): void {
  expect(rejectionReason(raw)).toContain(fragment);
}
