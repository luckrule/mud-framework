import { describe, expect, jest, test } from 'bun:test';

import { MAX_RESTART_BACKOFF_MS } from '../../shared/budget.ts';
import type { RestartPolicyOptions } from './restart-policy.ts';
import { RestartPolicy } from './restart-policy.ts';

const BASE_BACKOFF_MS = 100;
const STABLE_RESET_MS = 1000;

function makePolicy(
  options: Partial<RestartPolicyOptions> = {},
): RestartPolicy {
  return new RestartPolicy({
    baseBackoffMs: BASE_BACKOFF_MS,
    stableResetMs: STABLE_RESET_MS,
    ...options,
  });
}

/** Counts launches without reaching into the policy's timer fields. */
function countingLaunch(): { launch: () => void; calls: () => number } {
  let calls = 0;
  return {
    launch: () => {
      calls += 1;
    },
    calls: () => calls,
  };
}

describe('RestartPolicy backoff', () => {
  test('the first restart of a streak is immediate', () => {
    const policy = makePolicy();

    expect(policy.nextBackoffMs()).toBe(0);
    expect(policy.attempts).toBe(1);
  });

  test('later restarts double from the base', () => {
    const policy = makePolicy();

    const backoffs = [
      policy.nextBackoffMs(),
      policy.nextBackoffMs(),
      policy.nextBackoffMs(),
      policy.nextBackoffMs(),
    ];

    expect(backoffs).toEqual([0, 100, 200, 400]);
    expect(policy.attempts).toBe(4);
  });

  test('the backoff stops doubling at the cap', () => {
    const policy = makePolicy();

    const backoffs = Array.from({ length: 10 }, () => policy.nextBackoffMs());

    expect(backoffs).toEqual([
      0,
      100,
      200,
      400,
      800,
      1600,
      3200,
      MAX_RESTART_BACKOFF_MS,
      MAX_RESTART_BACKOFF_MS,
      MAX_RESTART_BACKOFF_MS,
    ]);
    expect(policy.attempts).toBe(10);
  });

  test('a base above the cap is capped on the second restart', () => {
    const policy = makePolicy({ baseBackoffMs: MAX_RESTART_BACKOFF_MS * 2 });

    expect(policy.nextBackoffMs()).toBe(0);
    expect(policy.nextBackoffMs()).toBe(MAX_RESTART_BACKOFF_MS);
  });
});

describe('RestartPolicy stability window', () => {
  test('the streak survives until the window expires', () => {
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      policy.nextBackoffMs();
      policy.nextBackoffMs();

      policy.startStableClock();
      jest.advanceTimersByTime(STABLE_RESET_MS - 1);
      expect(policy.attempts).toBe(2);

      jest.advanceTimersByTime(1);
      expect(policy.attempts).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a streak that ends restarts from the immediate backoff', () => {
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      policy.nextBackoffMs();
      policy.nextBackoffMs();
      policy.startStableClock();

      jest.advanceTimersByTime(STABLE_RESET_MS);

      expect(policy.nextBackoffMs()).toBe(0);
      expect(policy.attempts).toBe(1);
    } finally {
      jest.useRealTimers();
    }
  });

  test('restarting the stable clock postpones the reset', () => {
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      policy.nextBackoffMs();
      policy.startStableClock();

      jest.advanceTimersByTime(STABLE_RESET_MS / 2);
      policy.startStableClock();
      // The superseded clock must not stay armed.
      expect(jest.getTimerCount()).toBe(1);

      jest.advanceTimersByTime(STABLE_RESET_MS / 2);
      expect(policy.attempts).toBe(1);

      jest.advanceTimersByTime(STABLE_RESET_MS / 2);
      expect(policy.attempts).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });

  test('clearing the stable clock keeps the streak', () => {
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      policy.nextBackoffMs();
      policy.startStableClock();

      policy.clearStableClock();
      jest.advanceTimersByTime(STABLE_RESET_MS * 2);

      expect(policy.attempts).toBe(1);
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });

  test('clearing an expired stable clock changes nothing', () => {
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      policy.nextBackoffMs();
      policy.startStableClock();
      jest.advanceTimersByTime(STABLE_RESET_MS);

      expect(() => policy.clearStableClock()).not.toThrow();
      expect(policy.attempts).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });
});

describe('RestartPolicy restart scheduling', () => {
  test('runs the launch once after the backoff', () => {
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      const { launch, calls } = countingLaunch();

      policy.scheduleRestart(250, launch);
      jest.advanceTimersByTime(249);
      expect(calls()).toBe(0);

      jest.advanceTimersByTime(1);
      expect(calls()).toBe(1);

      jest.advanceTimersByTime(10_000);
      expect(calls()).toBe(1);
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });

  test('an immediate restart still waits for the timer', () => {
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      const { launch, calls } = countingLaunch();

      policy.scheduleRestart(0, launch);
      expect(calls()).toBe(0);

      jest.advanceTimersByTime(1);
      expect(calls()).toBe(1);
    } finally {
      jest.useRealTimers();
    }
  });

  test('rescheduling does not cancel the earlier restart', () => {
    // The policy keeps only the latest handle and never clears the timer it
    // replaces: callers that want one restart must clear first. Locking this
    // keeps a silent dedupe from passing as an implementation detail.
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      const first = countingLaunch();
      const second = countingLaunch();

      policy.scheduleRestart(100, first.launch);
      jest.advanceTimersByTime(50);
      policy.scheduleRestart(100, second.launch);

      jest.advanceTimersByTime(50);
      expect(first.calls()).toBe(1);
      expect(second.calls()).toBe(0);

      jest.advanceTimersByTime(50);
      expect(second.calls()).toBe(1);
    } finally {
      jest.useRealTimers();
    }
  });

  test('clearing the restart timer prevents the launch', () => {
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      const { launch, calls } = countingLaunch();

      policy.scheduleRestart(100, launch);
      policy.clearRestartTimer();
      jest.advanceTimersByTime(10_000);

      expect(calls()).toBe(0);
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });

  test('clearing an unarmed restart timer is a no-op', () => {
    const policy = makePolicy();

    expect(() => policy.clearRestartTimer()).not.toThrow();
    // A later schedule still arms and runs.
    jest.useFakeTimers();
    try {
      const { launch, calls } = countingLaunch();
      policy.scheduleRestart(100, launch);
      jest.advanceTimersByTime(100);
      expect(calls()).toBe(1);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a launch can arm the next restart', () => {
    // The handle is cleared before the callback runs, so a launch that re-arms
    // is not clobbered when the callback returns.
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      let calls = 0;
      const launch = (): void => {
        calls += 1;
        if (calls === 1) {
          policy.scheduleRestart(100, launch);
        }
      };

      policy.scheduleRestart(100, launch);
      jest.advanceTimersByTime(100);
      expect(calls).toBe(1);

      jest.advanceTimersByTime(100);
      expect(calls).toBe(2);
    } finally {
      jest.useRealTimers();
    }
  });

  test('clearing the restart timer leaves the stable clock running', () => {
    jest.useFakeTimers();
    try {
      const policy = makePolicy();
      const { launch, calls } = countingLaunch();
      policy.nextBackoffMs();
      policy.startStableClock();
      policy.scheduleRestart(100, launch);

      policy.clearRestartTimer();
      jest.advanceTimersByTime(STABLE_RESET_MS);

      expect(calls()).toBe(0);
      expect(policy.attempts).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });
});
