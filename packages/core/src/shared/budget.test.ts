import { describe, expect, test } from 'bun:test';

import { MAX_TIMER_DELAY_MS } from '../portal/connection-layer-options.ts';
import {
  DEFAULT_MAX_PENDING,
  DEFAULT_MAX_REPLAYS,
  DEFAULT_MAX_RESTART_BACKOFF_MS,
  DEFAULT_RESTART_BACKOFF_MS,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
  EXIT_WAIT_TIMEOUT_MS,
  SHUTDOWN_ACK_SLACK_MS,
  closeBudget,
  resolveStableResetMs,
} from './budget.ts';

describe('closeBudget', () => {
  test('the default shutdown timeout gives the ADR-0004 budget', () => {
    // switch abort 5000 + drain 5000 + grace (5000 + 250) + exit wait 2000 +
    // post-kill wait 2000 + abandoned exit wait (2000 * 2).
    expect(closeBudget(DEFAULT_SHUTDOWN_TIMEOUT_MS)).toBe(23_250);
  });

  test('the six waits are summed, not folded into one multiplier', () => {
    expect(closeBudget(5000)).toBe(5000 + 5000 + 5250 + 2000 + 2000 + 2000 * 2);
  });

  test('the budget grows linearly with the shutdown timeout', () => {
    // The switch abort, the drain and the grace depend on it; the exit waits
    // are fixed.
    const step = 1000;
    const fixed = EXIT_WAIT_TIMEOUT_MS * 4 + SHUTDOWN_ACK_SLACK_MS;
    for (const shutdownTimeoutMs of [1, 30, 500, 5000, 60_000]) {
      expect(closeBudget(shutdownTimeoutMs)).toBe(
        shutdownTimeoutMs * 3 + fixed,
      );
    }
    expect(closeBudget(step * 2) - closeBudget(step)).toBe(step * 3);
  });

  test('the fixed part is the four exit waits plus the ack slack', () => {
    expect(closeBudget(0)).toBe(
      EXIT_WAIT_TIMEOUT_MS * 4 + SHUTDOWN_ACK_SLACK_MS,
    );
  });
});

describe('budget constants', () => {
  // The values are pinned against ADR-0004: a retune here changes the close
  // contract every caller derives its own timeout from.
  test('match the defaults recorded in ADR-0004', () => {
    expect(DEFAULT_SHUTDOWN_TIMEOUT_MS).toBe(5000);
    expect(EXIT_WAIT_TIMEOUT_MS).toBe(2000);
    expect(SHUTDOWN_ACK_SLACK_MS).toBe(250);
  });

  test('restart backoff and replay limits match ADR-0004', () => {
    expect(DEFAULT_MAX_REPLAYS).toBe(3);
    expect(DEFAULT_RESTART_BACKOFF_MS).toBe(100);
    expect(DEFAULT_MAX_RESTART_BACKOFF_MS).toBe(5000);
  });

  test('the pending buffer cap matches ADR-0004', () => {
    expect(DEFAULT_MAX_PENDING).toBe(10_000);
  });
});

describe('resolveStableResetMs', () => {
  test('the 30s floor wins when the timeout and cap are smaller', () => {
    expect(resolveStableResetMs(5000, DEFAULT_MAX_RESTART_BACKOFF_MS)).toBe(
      30_000,
    );
  });

  test('the request timeout raises the window above the floor', () => {
    expect(resolveStableResetMs(60_000, DEFAULT_MAX_RESTART_BACKOFF_MS)).toBe(
      60_000,
    );
  });

  test('a raised cap raises the window so it still covers the backoff', () => {
    expect(resolveStableResetMs(5000, 45_000)).toBe(45_000);
  });

  test('the default window stays within the timer ceiling when the cap does', () => {
    expect(resolveStableResetMs(5000, MAX_TIMER_DELAY_MS)).toBe(
      MAX_TIMER_DELAY_MS,
    );
    expect(resolveStableResetMs(MAX_TIMER_DELAY_MS, 5000)).toBe(
      MAX_TIMER_DELAY_MS,
    );
  });

  test('only a timeout above the timer ceiling can push the window past it', () => {
    expect(resolveStableResetMs(MAX_TIMER_DELAY_MS + 1, 5000)).toBe(
      MAX_TIMER_DELAY_MS + 1,
    );
  });
});
