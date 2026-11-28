import { describe, expect, test } from 'bun:test';

import type { LogFields, Logger } from '../logging/types.ts';
import { EventDropLog } from './event-drop-log.ts';

interface Recorded {
  fields: LogFields;
  message: string;
}

function recordingLogger(): { logger: Logger; lines: Recorded[] } {
  const lines: Recorded[] = [];
  const logger: Logger = {
    debug: () => undefined,
    info: () => undefined,
    warn: (fields, message) => {
      lines.push({ fields, message });
    },
    error: () => undefined,
    child: () => logger,
    setLevel: () => undefined,
  };
  return { logger, lines };
}

/** A clock the test advances by hand, so no wall time enters the assertions. */
function manualClock(start = 0): {
  now: () => number;
  advance: (ms: number) => void;
} {
  let current = start;
  return {
    now: () => current,
    advance: (ms) => {
      current += ms;
    },
  };
}

describe('EventDropLog', () => {
  test('logs the first drop at once with the event that was dropped', () => {
    const { logger, lines } = recordingLogger();
    const log = new EventDropLog({ logger });

    log.record(
      { eventId: 'e:0', reason: 'no connection holds the session' },
      'no connection holds the session',
      'c1',
    );

    expect(lines).toHaveLength(1);
    expect(lines[0]?.fields).toMatchObject({
      eventId: 'e:0',
      reason: 'no connection holds the session',
      count: 1,
      droppedTotal: 1,
    });
  });

  test('folds a flood into a line per doubling and keeps the running total', () => {
    const { logger, lines } = recordingLogger();
    const clock = manualClock();
    const log = new EventDropLog({ logger, now: clock.now });

    for (let i = 0; i < 1024; i += 1) {
      log.record(
        { eventId: `e:${i}` },
        'no connection holds the session',
        'c1',
      );
    }

    // 1, 2, 4, ..., 1024: log2 growth, not 1024 lines.
    expect(lines).toHaveLength(11);
    expect(lines.map((line) => line.fields.droppedTotal)).toEqual([
      1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
    ]);
    // Each line's `count` is the batch since the previous line, so the counts
    // add up to every drop seen: a folded drop resurfaces in the next line.
    const total = lines.reduce(
      (sum, line) => sum + (line.fields.count as number),
      0,
    );
    expect(total).toBe(1024);
    // The first line names the dropped event; the folded ones do not claim to.
    expect(lines[0]?.fields.eventId).toBe('e:0');
  });

  test('separates targets and reasons, so one flood does not hide another', () => {
    const { logger, lines } = recordingLogger();
    const log = new EventDropLog({ logger });

    log.record({ eventId: 'a' }, 'no connection holds the session', 'c1');
    log.record({ eventId: 'b' }, 'no connection holds the session', 'c2');
    log.record({ eventId: 'c' }, 'target connection is not available', 'c1');

    expect(lines).toHaveLength(3);
    expect(lines.map((line) => line.fields.eventId)).toEqual(['a', 'b', 'c']);
  });

  test('reports the next drop at once after a quiet interval', () => {
    const { logger, lines } = recordingLogger();
    const clock = manualClock();
    const log = new EventDropLog({ logger, now: clock.now });

    // One line, then a long silence with a high cumulative count: without the
    // quiet reset the doubling rule would stay silent for a very long time.
    for (let i = 0; i < 8; i += 1) {
      log.record(
        { eventId: `e:${i}` },
        'no connection holds the session',
        'c1',
      );
    }
    expect(lines).toHaveLength(4);

    clock.advance(60_000);
    log.record({ eventId: 'later' }, 'no connection holds the session', 'c1');

    expect(lines).toHaveLength(5);
    // The quiet drop is reported on its own, not folded into a batch that would
    // grow until the next doubling.
    expect(lines.at(-1)?.fields).toMatchObject({
      eventId: 'later',
      count: 1,
      droppedTotal: 9,
    });
  });

  test('does not grow its bucket table without bound', () => {
    const { logger, lines } = recordingLogger();
    const clock = manualClock();
    const log = new EventDropLog({ logger, now: clock.now });

    // Far more distinct targets than the table can hold. Each new target is
    // still reported once, so the cap costs memory, not diagnosability.
    const targets = 5000;
    for (let i = 0; i < targets; i += 1) {
      log.record(
        { eventId: `e:${i}` },
        'no connection holds the session',
        `c${i}`,
      );
    }
    expect(lines).toHaveLength(targets);
    expect(lines.at(-1)?.fields.eventId).toBe(`e:${targets - 1}`);

    // A target evicted to make room is not silenced: its next drop starts a
    // fresh bucket and is reported at once, exactly as its first drop was.
    clock.advance(60_000);
    log.record({ eventId: 'again' }, 'no connection holds the session', 'c0');

    expect(lines).toHaveLength(targets + 1);
    expect(lines.at(-1)?.fields).toMatchObject({
      eventId: 'again',
      count: 1,
      droppedTotal: 1,
    });
  });

  test('evicts the least recently used bucket when the table is full', () => {
    const { logger, lines } = recordingLogger();
    const clock = manualClock();
    const log = new EventDropLog({ logger, now: clock.now });
    const reason = 'no connection holds the session';
    const drop = (target: string, eventId: string): void => {
      log.record({ eventId }, reason, target);
    };

    // Filling the table to MAX_BUCKETS (4096) leaves the next new target with
    // no room, so it has to evict one.
    for (let i = 0; i < 4096; i += 1) {
      drop(`c${i}`, `fill:${i}`);
    }
    // c0 was the oldest bucket; this drop makes it the most recent, so c1 is
    // now the least recently used one.
    drop('c0', 'c0:2');
    drop('c4096', 'new');

    // c1 lost its bucket: its next drop starts over and is reported at once,
    // instead of continuing from the count the evicted bucket had.
    drop('c1', 'c1:again');
    expect(lines.at(-1)?.fields).toMatchObject({
      eventId: 'c1:again',
      count: 1,
      droppedTotal: 1,
    });

    // c0 survived, so its bucket keeps accumulating and the next line still
    // stands for the whole batch.
    drop('c0', 'c0:3');
    drop('c0', 'c0:4');
    expect(lines.at(-1)?.fields).toMatchObject({
      eventId: 'c0:3',
      count: 2,
      droppedTotal: 4,
    });
  });
});
