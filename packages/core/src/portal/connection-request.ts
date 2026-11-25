import type { Envelope } from '../shared/codec.ts';
import type { ExecutorOutcome, RequestExecutor } from '../shared/executor.ts';
import { messageOf } from '../shared/guards.ts';
import { elapsedMs } from '../shared/timeout.ts';
import { TRANSPORT_CLOSED, failureLogLevel } from '../shared/wire-codes.ts';
import type { TransportConnection } from '../transport/types.ts';
import type { AdmissionHost, ConnectionState } from './connection-admission.ts';
import {
  refuseAfterShutdown,
  refuseQueue,
  refuseWithId,
} from './connection-admission.ts';
import { isExecutorOutcome, toFailure } from './connection-outcome-guards.ts';
import { rawLengthOf, rejectEnvelope, reply } from './connection-reply.ts';
import type { ConnectionLayerStatus } from './health.ts';
import type { SessionRouter } from './session-router.ts';

/**
 * What request handling needs from the layer: the admission view plus the three
 * collaborators a request touches (id minting, Session routing, execution).
 * `enqueue` is passed in so the connection's ordered chain has one owner.
 */
export interface RequestHost extends AdmissionHost {
  readonly createRequestId: () => string;
  readonly router: SessionRouter;
  readonly executor: RequestExecutor;
  readonly enqueue: (
    state: ConnectionState,
    work: () => Promise<void>,
  ) => Promise<void>;
}

export function onMessage(
  host: RequestHost,
  status: ConnectionLayerStatus,
  connection: TransportConnection,
  payload: string | Uint8Array,
): Promise<void> {
  if (status === 'stopping' || status === 'stopped') {
    // SPEC 6.3 step 1: the layer no longer accepts input. A stable code beats
    // silence, because the client is still waiting for an answer.
    return refuseAfterShutdown(host, connection, payload);
  }
  const state = host.connections.get(connection.id);
  if (state === undefined) {
    // Not a tracked connection (a race with stop, or a transport that
    // reconnects with a fresh object): there is nobody to answer.
    return Promise.resolve();
  }
  // Decode for correlation, but budget every ordered reply, including bad
  // frames: retaining uncounted callbacks would make this queue unbounded.
  let envelope: Envelope;
  try {
    envelope = host.codec.decode(payload);
  } catch (error) {
    if (state.queued >= host.maxQueuedPerConnection) {
      return refuseQueue(host, state, '');
    }
    const rawLength = rawLengthOf(payload);
    const reason = messageOf(error);
    state.queued += 1;
    return host.enqueue(state, async () => {
      state.queued -= 1;
      await rejectEnvelope(host, connection, rawLength, reason);
    });
  }
  // Assigned when the decoded request enters the framework, before the queue
  // cap decides whether it gets a seat: a refusal is still a Request whose log
  // line must be correlatable, while a malformed frame is not and never reaches
  // this point (SPEC 7.3).
  const requestId = host.createRequestId();
  if (state.queued >= host.maxQueuedPerConnection) {
    return refuseQueue(host, state, envelope.id, envelope.method, requestId);
  }
  state.queued += 1;
  return host.enqueue(state, () =>
    startAfterGate(host, state, envelope, requestId),
  );
}

async function startAfterGate(
  host: RequestHost,
  state: ConnectionState,
  envelope: Envelope,
  requestId: string,
): Promise<void> {
  const ran = await host.router.run(
    state.connection.id,
    state.closed.signal,
    async (sessionId, terminal) => {
      state.queued -= 1;
      await handle(
        host,
        state.connection,
        envelope,
        sessionId,
        terminal,
        requestId,
      );
    },
  );
  if (!ran) {
    state.queued -= 1;
    // Shutdown can stop waiting, but cannot start overlapping Session work.
    if (host.connections.get(state.connection.id) === state) {
      await refuseWithId(host, state.connection, envelope.id, requestId);
    }
  }
}

async function handle(
  host: RequestHost,
  connection: TransportConnection,
  envelope: Envelope,
  sessionId: string | undefined,
  terminal: Promise<void>,
  requestId: string,
): Promise<void> {
  const method = envelope.method;
  const clientRequestId = envelope.id;
  const requestPayload = envelope.payload;

  // The id was minted when the request entered the framework (SPEC 7.3), so
  // every line about it — including the executor's and the replay's — shares
  // it. The Session this connection already holds is read before the request
  // runs: a request that binds a Session still runs as unbound, and the binding
  // it declares is applied only after it succeeds. Every request line carries
  // it (SPEC 7.1), which is why the logger is built after this lookup.
  const state = host.connections.get(connection.id);
  const logger = host.logger.child({
    requestId,
    connectionId: connection.id,
    clientRequestId,
    ...(sessionId === undefined ? {} : { sessionId }),
  });
  const startedAt = performance.now();

  // A request queued behind another one can reach this point after its
  // connection closed. Executing it would commit writes for a player who is
  // gone, which is the same defect as running a cancelled request (SPEC 3.6),
  // so it is ended here instead — before it acquires a transaction.
  if (state === undefined) {
    logger.warn(
      { method, code: TRANSPORT_CLOSED, reason: 'connection closed' },
      'request dropped before start',
    );
    return;
  }
  // Registered before the executor is called, so a disconnect that lands while
  // the request is queued behind another one still cancels it.
  state.inFlight.add(requestId);
  let outcome: ExecutorOutcome;
  try {
    const raw: unknown = await host.executor.execute({
      requestId,
      connectionId: connection.id,
      clientRequestId,
      // The Session this connection already holds: the only source of
      // `RequestContext.sessionId` (SPEC 2) and of the Session ID on every
      // request line (SPEC 7.1). Read before the call, so a request that binds
      // a Session still runs as unbound — the binding it declares is applied
      // only after it succeeds.
      sessionId,
      method,
      payload: requestPayload,
    });
    // The guard exists because the executor is outside code: an outcome
    // without `error` would otherwise produce no frame at all, which is the
    // silent drop SPEC 3.5 forbids. A valid outcome is passed through as is;
    // the guard only checks the shape and rewrites no field.
    outcome = isExecutorOutcome(raw)
      ? raw
      : toFailure(new Error('executor returned a malformed outcome'));
  } catch (error) {
    // The interface forbids throwing, but an executor is outside code. An
    // escaping throw would reject `handle` and leave the client unanswered,
    // which is the silent drop SPEC 3.5 forbids, so it is mapped to the same
    // failure path as a returned `{ok:false}`.
    outcome = toFailure(error);
  } finally {
    state.inFlight.delete(requestId);
  }
  if (outcome.ok) {
    // SPEC 2: a Session change a handler declared takes effect only when the
    // request succeeded, so a failed login leaves no route behind. Applied
    // before the reply so a reconnect sees the new holder as soon as the client
    // can, and so the reply itself is part of the chain a later handoff waits
    // for.
    //
    // Skipped once the connection is gone: `onDisconnect` already removed its
    // route, and re-adding one here would route a future reconnect to a
    // connection id that no longer exists.
    const change = outcome.sessionChange;
    if (change !== undefined && host.connections.get(connection.id) === state) {
      if (change.type === 'bind') {
        host.router.bind(connection.id, change.sessionId, terminal, logger);
      } else {
        // Deliberately behind the same success check: a failed unbind keeps the
        // existing route. `unbind` leaves the Session's unfinished barrier in
        // place (INV-004). A Session this connection no longer holds belongs to
        // its successor, so the late unbind is dropped instead of silently
        // detaching the new holder (SPEC 3.2).
        const unbound = host.router.unbind(connection.id, sessionId, logger);
        if (unbound === 'held-by-other' && sessionId !== undefined) {
          logger.warn(
            {
              connectionId: connection.id,
              sessionId,
              code: 'SESSION_CHANGE_DROPPED',
              reason: 'session was taken over by another connection',
            },
            'session change dropped',
          );
        }
      }
    }
    await reply(
      host,
      connection,
      { id: clientRequestId, ok: true, result: outcome.result },
      logger,
    );
    return;
  }
  // An untracked connection means `onDisconnect` already ran: the request ended
  // because the client left, which is expected, and nobody can read a reply.
  // Writing one would only add a TRANSPORT_SEND_FAILED error on top of the
  // cancel the executor already logged (SPEC 7.1).
  if (!host.connections.has(connection.id)) {
    logger.debug(
      {
        method,
        code: outcome.error.code,
        durationMs: elapsedMs(startedAt),
      },
      'request ended after the connection closed',
    );
    return;
  }
  // warn for a business refusal: it is an expected outcome the Extension decided
  // on, and SPEC 7.1 forbids recording those at error level. The IPC protocol
  // carries no category, so the shared framework-code list is the only judgement
  // both processes can apply (wire-codes.ts).
  logger[failureLogLevel(outcome.error)](
    {
      method,
      code: outcome.error.code,
      durationMs: elapsedMs(startedAt),
      reason: outcome.error.message,
    },
    'request failed',
  );
  await reply(
    host,
    connection,
    {
      id: clientRequestId,
      ok: false,
      // The wire contract is `{code, message}` (SPEC 7.2); the category is
      // internal and must not become a client-visible field.
      error: { code: outcome.error.code, message: outcome.error.message },
    },
    logger,
  );
}

export async function cancelQuietly(
  host: RequestHost,
  requestId: string,
  connectionId: string,
): Promise<void> {
  try {
    await host.executor.cancel(requestId, TRANSPORT_CLOSED);
  } catch (error) {
    // The interface forbids throwing, but an executor is outside code. One
    // failed cancel must not keep the remaining requests on this connection
    // alive, and there is no caller to hand the failure to.
    host.logger.error(
      {
        requestId,
        connectionId,
        code: TRANSPORT_CLOSED,
        reason: messageOf(error),
      },
      'request could not be cancelled',
    );
  }
}
