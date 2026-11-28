import { describe, expect, test } from 'bun:test';

import type { LogFields, LogLevel, Logger } from '../logging/types.ts';
import { JsonCodec } from '../shared/codec.ts';
import type { ServerEvent } from '../shared/events.ts';
import type { ConnectionState } from './connection-state.ts';
import { EventBacklog } from './event-backlog.ts';
import { EventDropLog } from './event-drop-log.ts';
import type { EventDeliveryHost } from './event-frame.ts';
import { enqueueSessionEvent } from './session-event-queue.ts';
import type { SessionEventQueue } from './session-queue-state.ts';
import { SessionRouter } from './session-router.ts';

interface Recorded {
  level: LogLevel;
  fields: LogFields;
  message: string;
}

function recordingLogger(): { logger: Logger; lines: Recorded[] } {
  const lines: Recorded[] = [];
  const record =
    (level: LogLevel) =>
    (fields: LogFields, message: string): void => {
      lines.push({ level, fields, message });
    };
  const logger: Logger = {
    debug: record('debug'),
    info: record('info'),
    warn: record('warn'),
    error: record('error'),
    child: () => logger,
    setLevel: () => undefined,
  };
  return { logger, lines };
}

describe('Session event queue', () => {
  test('a pump that throws discards the queue, resolves accepted events and reports once', async () => {
    const { logger, lines } = recordingLogger();
    const backlog = new EventBacklog({
      logger,
      warnBytes: 1_000_000,
      liveConnections: () => 0,
    });
    const failure = new Error('acceptingEvents exploded');
    let acceptingCalls = 0;
    // The event is accepted into `entries` before the pump's first acceptance
    // check, so throwing there leaves a queued frame whose `done` the failure
    // path has to resolve and whose backlog charge it has to remove.
    const host: EventDeliveryHost = {
      codec: new JsonCodec(),
      logger,
      connections: new Map<string, ConnectionState>(),
      maxQueuedEvents: 10,
      maxQueuedEventBytes: 1_000_000,
      backlog,
      dropLog: new EventDropLog({ logger }),
      router: new SessionRouter({ logger }),
      queueEvent: () => ({ accepted: true }),
      cancelEvent: () => undefined,
      closeForBacklog: () => false,
      acceptingEvents: () => {
        acceptingCalls += 1;
        if (acceptingCalls === 1) {
          throw failure;
        }
        return true;
      },
      sessionQueues: new Map<string, SessionEventQueue>(),
      sendTimeoutMs: 1000,
    };
    const event: ServerEvent = {
      eventId: 'r1:0',
      event: 'room:update',
      payload: null,
      target: { kind: 'session', sessionId: 's1' },
    };

    // A pump failure must not leave this promise pending: whoever emitted the
    // event is waiting on it.
    await enqueueSessionEvent(host, 's1', event, {
      frame: '{"kind":"event"}',
      bytes: 17,
    });

    // Nothing is left holding the Session's serial slot or its frames.
    expect(host.sessionQueues.size).toBe(0);
    expect(backlog.snapshot()).toMatchObject({ frames: 0, bytes: 0 });
    const errors = lines.filter((line) => line.level === 'error');
    expect(errors).toHaveLength(1);
    expect(errors[0]?.fields).toMatchObject({
      code: 'EVENT_DELIVERY_FAILED',
      sessionId: 's1',
      reason: 'acceptingEvents exploded',
    });
  });
});
