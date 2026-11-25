import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { tick } from '../testing/common.ts';
import {
  degradationWarnings,
  echoRegistration,
  requestMessage,
  results,
  startServer,
} from '../testing/server-process.ts';

describe('runServerProcess capability degradation warnings', () => {
  test('a full-strength declaration produces no degradation warning', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: {
        transactions: 'full',
        concurrency: 'pooled',
        snapshotRead: true,
        uniqueConstraint: true,
      },
    });
    const harness = startServer({ backend });
    await tick();

    expect(degradationWarnings(harness.logs())).toEqual([]);
    await harness.stop();
  });

  test('a single-concurrency backend warns once with the fields', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: {
        transactions: 'full',
        concurrency: 'single',
        snapshotRead: true,
        uniqueConstraint: true,
      },
    });
    const harness = startServer({ backend });
    await tick();

    const warnings = degradationWarnings(harness.logs());
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatchObject({
      level: 'warn',
      capability: 'concurrency',
      declared: 'single',
    });
    expect(typeof warnings[0]?.consequence).toBe('string');
    await harness.stop();
  });

  test('four weak dimensions produce four warnings in a fixed order', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: {
        transactions: 'none',
        concurrency: 'single',
        snapshotRead: false,
        uniqueConstraint: false,
      },
    });
    const harness = startServer({ backend });
    await tick();

    const warnings = degradationWarnings(harness.logs());
    expect(warnings).toHaveLength(4);
    expect(warnings.map((line) => line.capability)).toEqual([
      'transactions',
      'concurrency',
      'snapshotRead',
      'uniqueConstraint',
    ]);
    expect(warnings.every((line) => line.level === 'warn')).toBe(true);
    await harness.stop();
  });

  test('the warnings are emitted at startup, not once per request', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: {
        transactions: 'full',
        concurrency: 'single',
        snapshotRead: true,
        uniqueConstraint: true,
      },
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();
    // The handshake already ran; a second `hello` would only log STALE_HELLO.
    const atStartup = degradationWarnings(harness.logs()).length;

    for (let index = 0; index < 3; index += 1) {
      harness.portal.send(requestMessage({ requestId: `req-${index + 1}` }));
    }
    await tick();

    expect(results(harness.messages)).toHaveLength(3);
    // Log volume must not scale with traffic (SPEC 4.3, 7.1).
    expect(degradationWarnings(harness.logs())).toHaveLength(atStartup);
    expect(atStartup).toBe(1);
    await harness.stop();
  });
});
