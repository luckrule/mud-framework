import { describe, expect, test } from 'bun:test';

import {
  DEFAULT_MAX_PENDING,
  DEFAULT_MAX_REPLAYS,
  DEFAULT_RESTART_BACKOFF_MS,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
  EXIT_WAIT_TIMEOUT_MS,
  MAX_RESTART_BACKOFF_MS,
  SHUTDOWN_ACK_SLACK_MS,
  closeBudget,
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
    expect(MAX_RESTART_BACKOFF_MS).toBe(5000);
  });

  test('the pending buffer cap matches ADR-0004', () => {
    expect(DEFAULT_MAX_PENDING).toBe(10_000);
  });
});
