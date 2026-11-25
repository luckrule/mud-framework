import { describe, expect, test } from 'bun:test';

import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { rejectionOf } from '../testing/common.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { received } from '../testing/server-process.ts';
import { runServerProcess } from './server-process.ts';

/**
 * Stands in for a channel whose backlog replay throws inside `onMessage`: the
 * in-memory channel delivers queued messages synchronously, so a throwing
 * handler surfaces at the registration call instead of on the event loop.
 *
 * The first registration is the pre-activation handler and must succeed; the
 * second one is the post-`ready` handler this test is about (ADR-0007).
 */
function channelThatRejectsSecondRegistration(inner: IpcChannel): IpcChannel {
  let registrations = 0;
  return {
    send: (message) => inner.send(message),
    onMessage: (handler) => {
      registrations += 1;
      if (registrations > 1) {
        throw new Error('registration refused');
      }
      inner.onMessage(handler);
    },
    onClose: (handler) => inner.onClose(handler),
    close: (reason) => inner.close(reason),
  };
}

describe('runServerProcess inbound registration', () => {
  test('closes the backend when attaching the handlers throws', async () => {
    const [portal, serverChannel] = createInMemoryChannelPair();
    const messages: unknown[] = [];
    portal.onMessage((raw) => {
      messages.push(raw);
      // Drive the two-phase handshake so the failure lands on the second
      // `onMessage` registration, after `ready`.
      const parsed = parseIpcMessage(raw);
      if (parsed.ok && parsed.message.type === 'prepared') {
        portal.send({ type: 'activate' });
      }
    });
    const backend = new MemoryStorageBackend();
    const done = runServerProcess({
      createBackend: () => backend,
      extensions: [],
      timeoutMs: 1000,
      logger: createJsonLogger({ sink: () => undefined }),
      channel: channelThatRejectsSecondRegistration(serverChannel),
    });
    portal.send({ type: 'hello' });

    const error = await rejectionOf(done);

    expect(error.message).toBe('registration refused');
    // The barrier is crossed only after the handler is installed, so a
    // registration that throws means Portal never saw `ready`.
    expect(received(messages).map((message) => message.type)).toEqual([
      'prepared',
    ]);
    // The backend is the only storage writer (INV-002): a registration that
    // throws must not leave it open until the process exits.
    expect(() => backend.transaction(() => Promise.resolve())).toThrow(
      /storage backend is closed/,
    );
  });
});
