import { describe, expect, test } from 'bun:test';

import { portalStopFailed, shouldRemoveDataDir } from './cleanup.ts';

describe('benchmark data directory cleanup', () => {
  test('removes only after a confirmed clean stop with no survivors', () => {
    expect(
      shouldRemoveDataDir({
        stopFailed: false,
        survivors: [],
        keep: false,
      }),
    ).toBe(true);
  });

  test('keeps the directory when the stop was not confirmed', () => {
    expect(
      shouldRemoveDataDir({
        stopFailed: true,
        survivors: [],
        keep: false,
      }),
    ).toBe(false);
  });

  test('keeps the directory while any child pid survives', () => {
    expect(
      shouldRemoveDataDir({
        stopFailed: false,
        survivors: [1234],
        keep: false,
      }),
    ).toBe(false);
  });

  test('keeps the directory when the user asked to keep it', () => {
    expect(
      shouldRemoveDataDir({
        stopFailed: false,
        survivors: [],
        keep: true,
      }),
    ).toBe(false);
  });
});

describe('benchmark Portal stop classification', () => {
  test('a clean zero exit with no survivors is not a failure', () => {
    expect(
      portalStopFailed({ exitCode: 0, forced: false, survivors: [] }),
    ).toBe(false);
  });

  test('a non-zero exit keeps the directory', () => {
    expect(
      portalStopFailed({ exitCode: 1, forced: false, survivors: [] }),
    ).toBe(true);
  });

  test('an exit the bounded wait never observed keeps the directory', () => {
    expect(
      portalStopFailed({ exitCode: undefined, forced: false, survivors: [] }),
    ).toBe(true);
  });

  test('a signal exit with no survivors is an observed exit', () => {
    // The process is gone even though there is no exit code to report; no
    // live writer holds the SQLite file, so the directory is safe to remove.
    expect(
      portalStopFailed({ exitCode: null, forced: false, survivors: [] }),
    ).toBe(false);
  });

  test('a forced stop is non-graceful even when nothing survived', () => {
    expect(
      portalStopFailed({ exitCode: null, forced: true, survivors: [] }),
    ).toBe(true);
  });

  test('a survivor keeps the directory', () => {
    expect(
      portalStopFailed({ exitCode: 0, forced: false, survivors: [1234] }),
    ).toBe(true);
  });
});
