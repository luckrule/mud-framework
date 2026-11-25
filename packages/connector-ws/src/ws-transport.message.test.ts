import { describe, expect, test } from 'bun:test';
import { Buffer } from 'node:buffer';

import type {
  TransportConnection,
  TransportHandlers,
} from 'mud-framework-core';
import { openClient, tick as settle } from 'mud-framework-test-support';

import type { FakeSocket } from './ws-transport.support.ts';
import {
  captureLogger,
  captureProcessFailures,
  closeRecorder,
  first,
  recorder,
  safeStop,
  startTransport,
  until,
} from './ws-transport.support.ts';
import { WsTransport } from './ws-transport.ts';

describe('WsTransport messages', () => {
  test('delivers a text message as a string', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      client.send('look');
      await until(() => recorded.messages.length === 1, 'onMessage');

      const received = first(recorded.messages);
      expect(received.payload).toBe('look');
      expect(received.connection).toBe(first(recorded.connected));
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('delivers a binary message as a plain Uint8Array', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      client.send(new Uint8Array([1, 2, 255]));
      await until(() => recorded.messages.length === 1, 'onMessage');

      const { payload } = first(recorded.messages);
      if (!(payload instanceof Uint8Array)) {
        throw new Error('expected a binary payload');
      }
      expect(Buffer.isBuffer(payload)).toBe(false);
      expect([...payload]).toEqual([1, 2, 255]);
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('closes the connection when onMessage rejects, without escaping', async () => {
    const { logger, entries } = captureLogger();
    const failures = captureProcessFailures();
    const { handlers, recorded } = recorder();
    handlers.onMessage = () =>
      Promise.reject(new Error('message handler boom'));
    const transport = new WsTransport({ logger });
    await transport.start(handlers);
    const port = transport.port;
    if (port === undefined) {
      throw new Error('expected the transport to bind a port');
    }
    const client = await openClient(`ws://127.0.0.1:${port}/`);
    let closeCode: number | undefined;
    client.addEventListener('close', (event) => {
      closeCode = event.code;
    });
    try {
      await until(() => recorded.connected.length === 1, 'onConnect');
      client.send('look');
      await settle();

      expect(failures.unhandledRejections).toHaveLength(0);
      expect(failures.uncaughtExceptions).toHaveLength(0);

      await until(() => closeCode !== undefined, 'the connection to close');
      expect(closeCode).toBe(1011);

      const failure = entries().find(
        (entry) => entry.code === 'TRANSPORT_HANDLER_FAILED',
      );
      expect(failure).toMatchObject({
        level: 'error',
        connectionId: first(recorded.connected).id,
        reason: 'message handler boom',
      });
      // The failure close still runs the disconnect path exactly once.
      expect(recorded.disconnected).toHaveLength(1);
    } finally {
      failures.stop();
      client.close();
      await safeStop(transport);
    }
  });

  test('closes the connection when onConnect throws, without escaping', async () => {
    const { logger, entries } = captureLogger();
    const failures = captureProcessFailures();
    const transport = new WsTransport({ logger });
    const handlers: TransportHandlers = {
      onConnect: () => {
        throw new Error('connect handler boom');
      },
      onMessage: () => undefined,
      onDisconnect: () => undefined,
    };
    await transport.start(handlers);
    const port = transport.port;
    if (port === undefined) {
      throw new Error('expected the transport to bind a port');
    }
    const client = new WebSocket(`ws://127.0.0.1:${port}/`);
    let closeCode: number | undefined;
    client.addEventListener('close', (event) => {
      closeCode = event.code;
    });
    try {
      await until(() => closeCode !== undefined, 'the connection to close');
      await settle();

      expect(closeCode).toBe(1011);
      expect(failures.unhandledRejections).toHaveLength(0);
      expect(failures.uncaughtExceptions).toHaveLength(0);

      const failure = entries().find(
        (entry) => entry.code === 'TRANSPORT_HANDLER_FAILED',
      );
      expect(failure).toMatchObject({
        level: 'error',
        reason: 'connect handler boom',
      });
      expect(failure?.connectionId).toBeString();
    } finally {
      failures.stop();
      client.close();
      await safeStop(transport);
    }
  });

  test('logs a rejected onDisconnect without escaping the process', async () => {
    const { logger, entries } = captureLogger();
    const failures = captureProcessFailures();
    const disconnected: TransportConnection[] = [];
    const transport = new WsTransport({ logger });
    await transport.start({
      onConnect: () => undefined,
      onMessage: () => undefined,
      onDisconnect: (connection) => {
        disconnected.push(connection);
        return Promise.reject(new Error('disconnect handler boom'));
      },
    });
    const port = transport.port;
    if (port === undefined) {
      throw new Error('expected the transport to bind a port');
    }
    const client = await openClient(`ws://127.0.0.1:${port}/`);
    try {
      client.close(1000, 'bye');
      await until(
        () =>
          entries().some((entry) => entry.code === 'TRANSPORT_HANDLER_FAILED'),
        'the handler failure log',
      );
      await settle();

      expect(failures.unhandledRejections).toHaveLength(0);
      expect(failures.uncaughtExceptions).toHaveLength(0);
      expect(disconnected).toHaveLength(1);
    } finally {
      failures.stop();
      client.close();
      await safeStop(transport);
    }
  });

  test('warns and closes a late event for an unknown connection id', async () => {
    const { logger, entries } = captureLogger();
    const transport = new WsTransport({ logger });
    const { ws, closes } = closeRecorder('missing');
    const internals = transport as unknown as {
      handleMessage(ws: FakeSocket, message: string): void | Promise<void>;
      handleOpen(ws: FakeSocket): void | Promise<void>;
    };

    await internals.handleMessage(ws, 'late');
    await internals.handleOpen(ws);

    expect(closes).toHaveLength(2);
    expect(closes.every((close) => close.code === 1011)).toBe(true);
    const warnings = entries().filter(
      (entry) => entry.code === 'TRANSPORT_UNKNOWN_CONNECTION',
    );
    expect(warnings).toHaveLength(2);
    expect(warnings[0]).toMatchObject({
      level: 'warn',
      transport: 'ws',
      connectionId: 'missing',
    });
  });
});
