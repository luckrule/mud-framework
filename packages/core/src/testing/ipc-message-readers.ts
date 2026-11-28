import { expect } from 'bun:test';

import type { IpcMessage, RequestMessage } from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { FakeServerProcess } from './fake-server-process.ts';

export function messagesOf(server: FakeServerProcess): IpcMessage[] {
  return server.received.flatMap((raw) => {
    const parsed = parseIpcMessage(raw);
    if (!parsed.ok) {
      throw new Error(`portal sent an unparsable message: ${parsed.reason}`);
    }
    return [parsed.message];
  });
}

export function requestMessages(server: FakeServerProcess): RequestMessage[] {
  return messagesOf(server).flatMap((message) =>
    message.type === 'request' ? [message] : [],
  );
}

export function requestIds(server: FakeServerProcess): string[] {
  return requestMessages(server).map((message) => message.requestId);
}

/** Asserts the server received exactly one message and that it is `hello`. */
export function expectOnlyHello(server: FakeServerProcess): void {
  const messages = messagesOf(server);
  expect(messages).toHaveLength(1);
  const [first] = messages;
  if (first?.type !== 'hello') {
    throw new Error(`expected hello, got ${first?.type ?? 'no message'}`);
  }
}

/**
 * Asserts the completed two-phase handshake and nothing else: `hello` followed
 * by `activate` (ADR-0007). A request at this point would mean the Portal
 * delivered before `ready`.
 */
export function expectHandshake(server: FakeServerProcess): void {
  const messages = messagesOf(server);
  expect(messages.map((message) => message.type)).toEqual([
    'hello',
    'activate',
  ]);
}
