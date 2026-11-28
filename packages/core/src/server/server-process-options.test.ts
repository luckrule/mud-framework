import { describe, expect, test } from 'bun:test';

import type { ServerProcessOptions } from '../index.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import type { ServerProcessTestOptions } from './server-process-options.ts';

function productionOptions(): ServerProcessOptions {
  return {
    createBackend: () => new MemoryStorageBackend(),
    extensions: [],
    timeoutMs: 1000,
  };
}

describe('ServerProcessOptions public shape', () => {
  test('rejects the test-only overrides', () => {
    // Each literal is separate so every `@ts-expect-error` has exactly one
    // excess-property error to consume; the typecheck gate fails on an unused
    // directive, which is the assertion here.
    const withAbort: ServerProcessOptions = {
      ...productionOptions(),
      // @ts-expect-error -- abortTimeoutMs is test-only, not public contract
      abortTimeoutMs: 10,
    };
    const withInterval: ServerProcessOptions = {
      ...productionOptions(),
      // @ts-expect-error -- parentCheckIntervalMs is test-only
      parentCheckIntervalMs: 0,
    };
    const withReadParent: ServerProcessOptions = {
      ...productionOptions(),
      // @ts-expect-error -- readParentPid is test-only
      readParentPid: () => 1,
    };

    expect(withAbort.timeoutMs).toBe(1000);
    expect(withInterval.timeoutMs).toBe(1000);
    expect(withReadParent.timeoutMs).toBe(1000);
  });

  test('the internal test-options type still accepts them', () => {
    const options: ServerProcessTestOptions = {
      ...productionOptions(),
      abortTimeoutMs: 10,
      parentCheckIntervalMs: 0,
      readParentPid: () => 1234,
    };

    expect(options.parentCheckIntervalMs).toBe(0);
    expect(options.readParentPid?.()).toBe(1234);
  });
});
