import { eventMessage } from '../ipc/outbound.ts';
import type { IpcMessage, RequestMessage } from '../ipc/protocol.ts';
import type { LogFields, Logger } from '../logging/types.ts';
import type {
  BufferedServerEvent,
  ServerEventTarget,
} from '../shared/events.ts';
import type { ExecutorOutcome } from '../shared/executor.ts';
import { failureLogLevel } from '../shared/wire-codes.ts';

/**
 * The write half of the runner's dependencies. Result writing needs only the
 * logger and the send function: the in-flight bookkeeping stays in the runner.
 */
export interface ResultWriterDeps {
  readonly logger: Logger;
  readonly send: (message: IpcMessage, what: string) => boolean;
}

/** Returns whether the result message was actually written. */
export function report(
  deps: ResultWriterDeps,
  message: RequestMessage,
  baseFields: LogFields,
  outcome: ExecutorOutcome,
  durationMs: number,
): boolean {
  if (outcome.ok) {
    const fields: LogFields = { ...baseFields, durationMs };
    const change = outcome.sessionChange;
    if (change !== undefined) {
      // `sessionId` already names the entry Session; the routing change is a
      // separate field so the two cannot be confused.
      fields.sessionChange = change;
    }
    deps.logger.debug(fields, 'request completed');
    // The outcome already has the shape of the message's payload half, so it
    // is spread rather than copied field by field. An unchanged success
    // carries `sessionChange: undefined` as an own property, which every
    // channel either drops (JSON) or keeps with the same meaning as absence
    // (SPEC 2). Events are not part of the result message (ADR-0008): they
    // follow it as separate messages, so the field is removed here.
    const { events: _events, ...wireOutcome } = outcome;
    return deps.send({ ...resultEnvelope(message), ...wireOutcome }, 'result');
  }
  deps.logger[failureLogLevel(outcome.error)](
    {
      ...baseFields,
      code: outcome.error.code,
      reason: outcome.error.message,
      durationMs,
    },
    'request failed',
  );
  return deps.send({ ...resultEnvelope(message), ...outcome }, 'result');
}

/**
 * Sends one request's events, in order, after its result (ADR-0008).
 *
 * Best effort: a failed result drops every event because the caller that
 * would interpret them never saw the request succeed; a single failed event
 * is logged and the rest are still attempted. Neither failure changes the
 * request's success, and nothing here waits.
 */
export function sendEvents(
  deps: ResultWriterDeps,
  message: RequestMessage,
  fields: LogFields,
  events: readonly BufferedServerEvent[] | undefined,
  resultSent: boolean,
): void {
  if (events === undefined || events.length === 0) {
    return;
  }
  if (!resultSent) {
    deps.logger.warn(
      {
        ...fields,
        code: 'EVENTS_DROPPED',
        count: events.length,
        reason: 'result could not be sent',
      },
      'events dropped with the unsent result',
    );
    return;
  }
  for (const [index, event] of events.entries()) {
    const eventId = `${message.requestId}:${index}`;
    if (!deps.send(eventMessage(eventId, event), 'event')) {
      // The entry identity's `sessionId` names the Session the request
      // entered with, not the event's delivery target; the target fields
      // carry a `target` prefix so the two cannot be read as the same thing
      // (SPEC 7.1).
      deps.logger.warn(
        {
          ...fields,
          code: 'EVENT_SEND_FAILED',
          eventId,
          event: event.event,
          ...eventTargetFields(event.target),
          reason: 'event could not be sent',
        },
        'event could not be sent',
      );
    }
  }
}

/**
 * The delivery target of one event as log fields (SPEC 7.1). `EVENT_SEND_FAILED`
 * lines also carry the request's entry identity, whose `sessionId` is the
 * Session the request entered with; the `target` prefix keeps that field from
 * being read as the delivery target.
 */
function eventTargetFields(target: ServerEventTarget): LogFields {
  return target.kind === 'connection'
    ? { targetKind: 'connection', targetConnectionId: target.connectionId }
    : { targetKind: 'session', targetSessionId: target.sessionId };
}

/**
 * The correlation half of every result sent for `message`: its request id plus
 * the Session it entered with, echoed so the Portal can name the Session of a
 * result that arrives after its pending entry is gone. The echo is not an
 * outcome field; a Session change declared by the handler stays in
 * `outcome.sessionChange`.
 */
export function resultEnvelope(message: RequestMessage): {
  type: 'result';
  requestId: string;
  requestSessionId?: string;
} {
  if (message.sessionId === undefined) {
    return { type: 'result', requestId: message.requestId };
  }
  return {
    type: 'result',
    requestId: message.requestId,
    requestSessionId: message.sessionId,
  };
}
