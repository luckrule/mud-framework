import { describe, expect, test } from 'bun:test';

import { shutdownPortal } from './shutdown.ts';

describe('minimal Portal shutdown', () => {
  test('a rejected layer stop still stops the health endpoint', async () => {
    const calls: string[] = [];
    const layerError = new Error('layer stop failed');
    const outcome = await shutdownPortal({
      stopLayer: async () => {
        calls.push('layer');
        throw layerError;
      },
      stopHealth: async () => {
        calls.push('health');
      },
    });

    // The HTTP listener holds the event loop; skipping health.stop leaves the
    // process alive after SIGTERM.
    expect(calls).toEqual(['layer', 'health']);
    expect(outcome.failures[0]?.error).toBe(layerError);
    expect(outcome.failures).toEqual([{ target: 'layer', error: layerError }]);
    expect(outcome.exitCode).toBe(1);
  });

  test('a health stop error does not mask the original layer error', async () => {
    const layerError = new Error('layer stop failed');
    const healthError = new Error('health stop failed');
    const outcome = await shutdownPortal({
      stopLayer: async () => {
        throw layerError;
      },
      stopHealth: async () => {
        throw healthError;
      },
    });

    expect(outcome.failures[0]?.error).toBe(layerError);
    expect(outcome.failures.map((failure) => failure.target)).toEqual([
      'layer',
      'health',
    ]);
    expect(outcome.exitCode).toBe(1);
  });

  test('reports a health-only failure', async () => {
    const healthError = new Error('health stop failed');
    const outcome = await shutdownPortal({
      stopLayer: async () => undefined,
      stopHealth: async () => {
        throw healthError;
      },
    });

    expect(outcome.failures[0]?.error).toBe(healthError);
    expect(outcome.exitCode).toBe(1);
  });

  test('both stops succeeding exits 0', async () => {
    const calls: string[] = [];
    const outcome = await shutdownPortal({
      stopLayer: async () => {
        calls.push('layer');
      },
      stopHealth: async () => {
        calls.push('health');
      },
    });

    expect(calls).toEqual(['layer', 'health']);
    expect(outcome.failures).toEqual([]);
    expect(outcome.exitCode).toBe(0);
  });
});
