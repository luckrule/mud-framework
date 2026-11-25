import { describe, expect, test } from 'bun:test';

import {
  openClient,
  rejectionOf,
  throwingLogger,
} from 'mud-framework-test-support';

import {
  clientFailure,
  first,
  recorder,
  safeStop,
  startTransport,
  until,
} from './ws-transport.support.ts';
import { WsTransport } from './ws-transport.ts';

describe('WsTransport lifecycle', () => {
  test('starts and stops with a logger whose every method throws', async () => {
    const { transport, recorded, url } = await startTransport({
      logger: throwingLogger(),
    });
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('defaults the message ceiling to one mebibyte', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      const oversize = new Uint8Array(1024 * 1024 + 1);
      const error = await rejectionOf(first(recorded.connected).send(oversize));

      expect(error.message).toMatch(/exceeds the 1048576 byte limit/);
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('has no port before start and a bound port after', async () => {
    const transport = new WsTransport();

    expect(transport.port).toBeUndefined();
    try {
      await transport.start(recorder().handlers);
      expect(transport.port).toBeGreaterThan(0);
    } finally {
      await safeStop(transport);
    }
  });

  test('reports the connection to onConnect', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');
      client.close();

      expect(first(recorded.connected).id).toBeString();
    } finally {
      await safeStop(transport);
    }
  });

  test('reports the close code and reason to onDisconnect', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      client.close(1000, 'bye');
      await until(() => recorded.disconnected.length === 1, 'onDisconnect');

      expect(first(recorded.disconnected).reason).toBe('1000 bye');
    } finally {
      await safeStop(transport);
    }
  });

  test('reports a bare close as its status code', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      client.close();
      await until(() => recorded.disconnected.length === 1, 'onDisconnect');

      expect(first(recorded.disconnected).reason).toMatch(/^code 100[05]$/);
    } finally {
      await safeStop(transport);
    }
  });

  test('rejects a send above maxMessageBytes', async () => {
    const { transport, recorded, url } = await startTransport({
      maxMessageBytes: 8,
    });
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      const error = await rejectionOf(
        first(recorded.connected).send('x'.repeat(9)),
      );

      expect(error.message).toMatch(/exceeds/);
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('closes live connections and refuses work after stop', async () => {
    const { transport, recorded, url } = await startTransport();
    const client = await openClient(url);
    try {
      await until(() => recorded.connected.length === 1, 'onConnect');
      const connection = first(recorded.connected);
      const closed = new Promise<number>((resolve) => {
        client.addEventListener(
          'close',
          (event) => {
            resolve(event.code);
          },
          { once: true },
        );
      });

      await transport.stop();

      expect(await closed).toBe(1001);
      expect(transport.port).toBeUndefined();
      const sendError = await rejectionOf(connection.send('x'));
      expect(sendError.message).toMatch(/stopped/);
      const stopError = await rejectionOf(transport.stop());
      expect(stopError.message).toMatch(/cannot stop/);
    } finally {
      await safeStop(transport);
    }
  });

  test('refuses to stop before start or start twice', async () => {
    const transport = new WsTransport();

    const stopError = await rejectionOf(transport.stop());
    expect(stopError.message).toMatch(/cannot stop/);

    await transport.start(recorder().handlers);
    try {
      const startError = await rejectionOf(
        transport.start(recorder().handlers),
      );
      expect(startError.message).toMatch(/cannot start/);
    } finally {
      await safeStop(transport);
    }
  });

  test('rejects an upgrade for a non-matching path', async () => {
    const { transport, recorded, url } = await startTransport({ path: '/ws' });
    try {
      await clientFailure(url);

      expect(recorded.connected).toHaveLength(0);
    } finally {
      await safeStop(transport);
    }
  });

  test('answers a plain HTTP request with a clear status', async () => {
    const { transport } = await startTransport();
    try {
      const port = transport.port;
      if (port === undefined) {
        throw new Error('expected the transport to bind a port');
      }

      const response = await fetch(`http://127.0.0.1:${port}/`);
      await response.text();

      expect(response.status).toBe(426);
    } finally {
      await safeStop(transport);
    }
  });
});
