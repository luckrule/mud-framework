import { describe, expect, test } from 'bun:test';

import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf } from '../test-support/common.ts';
import { received } from '../test-support/server-process.ts';
import { runServerProcess } from './server-process.ts';

/**
 * Stands in for a channel whose backlog replay throws inside `onMessage`: the
 * in-memory channel delivers queued messages synchronously, so a throwing
 * handler surfaces at the registration call instead of on the event loop.
 */
function channelThatRejectsRegistration(inner: IpcChannel): IpcChannel {
  return {
    send: (message) => inner.send(message),
    onMessage: () => {
      throw new Error('registration refused');
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
    });
    const backend = new MemoryStorageBackend();
    const done = runServerProcess({
      createBackend: () => backend,
      extensions: [],
      timeoutMs: 1000,
      logger: createJsonLogger({ sink: () => undefined }),
      channel: channelThatRejectsRegistration(serverChannel),
    });

    const error = await rejectionOf(done);

    expect(error.message).toBe('registration refused');
    // The failure is after the readiness barrier, not a failed startup.
    expect(received(messages).map((message) => message.type)).toEqual([
      'ready',
    ]);
    // The backend is the only storage writer (INV-002): a registration that
    // throws must not leave it open until the process exits.
    expect(() => backend.transaction(() => Promise.resolve())).toThrow(
      /storage backend is closed/,
    );
  });
});
