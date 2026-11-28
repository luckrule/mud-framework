import { GameClient, GameClientError } from '../src/client-lib.ts';
import {
  CLIENT_COUNT,
  LOGOUT_TIMEOUT_MS,
  REQUEST_TIMEOUT_MS,
  RETRYABLE_CODES,
} from './config.ts';
import {
  asMs,
  fail,
  formatMs,
  messageOf,
  nowNs,
  readLogin,
  SerialQueue,
  withDeadline,
} from './support.ts';
import type { AttemptStats, CommandResult, DrillClient, Run } from './types.ts';

/** One client's stats, request attempts and connection lifecycle. */

export function emptyStats(): AttemptStats {
  return {
    attempts: 0,
    ok: 0,
    clientTimeouts: 0,
    frameworkTimeouts: 0,
    failures: 0,
    disconnects: 0,
    retried: 0,
    retrySucceeded: 0,
    recoveredMoves: 0,
    looks: 0,
    says: 0,
    moves: 0,
    latencyNs: [],
    codes: new Map(),
    repeatsByWindow: new Map(),
    firstAttemptWindow: new Map(),
  };
}

interface AttemptOutcome {
  readonly ok: boolean;
  readonly code: string;
  readonly message: string;
  readonly result: unknown;
}

/**
 * One attempt, counted even when it fails. A timeout is a hard failure on the
 * spot: a later retry cannot cancel the deadline the client already waited
 * out, and the full duration stays in the latency samples.
 */
async function attempt(
  run: Run,
  state: DrillClient,
  method: string,
  payload: unknown,
  commandId: string,
): Promise<AttemptOutcome> {
  const stats = state.stats;
  const firstWindow = stats.firstAttemptWindow.get(commandId);
  if (firstWindow === undefined) {
    stats.firstAttemptWindow.set(commandId, run.activeSwitch);
  } else {
    // Attribute the repeat to the window its first attempt belonged to: by the
    // time a retry runs, the switch is long over.
    stats.repeatsByWindow.set(
      firstWindow,
      (stats.repeatsByWindow.get(firstWindow) ?? 0) + 1,
    );
  }
  stats.attempts += 1;
  const startNs = nowNs();
  try {
    const result = await state.client.call(method, payload, commandId);
    const endNs = nowNs();
    stats.ok += 1;
    stats.latencyNs.push(endNs - startNs);
    run.requestSamples.push({ startNs, endNs, ok: true });
    return { ok: true, code: '', message: '', result };
  } catch (error) {
    const endNs = nowNs();
    const code = error instanceof GameClientError ? error.code : 'UNKNOWN';
    const message = messageOf(error);
    stats.codes.set(code, (stats.codes.get(code) ?? 0) + 1);
    stats.latencyNs.push(endNs - startNs);
    run.requestSamples.push({ startNs, endNs, ok: false });
    if (code === 'CLIENT_TIMEOUT' || code === 'FRAMEWORK_TIMEOUT') {
      if (code === 'CLIENT_TIMEOUT') {
        stats.clientTimeouts += 1;
      } else {
        stats.frameworkTimeouts += 1;
      }
      fail(
        run,
        `${state.name}: ${method} ${commandId} ${code} after ${formatMs(asMs(endNs - startNs))} ms`,
      );
    } else {
      stats.failures += 1;
      if (code === 'CLIENT_CLOSED' || code === 'CLIENT_SEND_FAILED') {
        stats.disconnects += 1;
        fail(
          run,
          `${state.name}: ${method} ${commandId} lost the connection (${code})`,
        );
      }
    }
    return { ok: false, code, message, result: undefined };
  }
}

function noteSuccess(stats: AttemptStats, method: string): void {
  if (method === 'game.look') {
    stats.looks += 1;
  } else if (method === 'game.say') {
    stats.says += 1;
  } else if (method === 'game.move') {
    stats.moves += 1;
  }
}

/** One logical command, retried at most once under the same client request id. */
export function command(
  run: Run,
  state: DrillClient,
  method: string,
  payload: unknown,
  commandId: string,
): Promise<CommandResult> {
  return state.queue.run(async () => {
    const first = await attempt(run, state, method, payload, commandId);
    if (first.ok) {
      noteSuccess(state.stats, method);
      return { ok: true, result: first.result, attempts: 1 };
    }
    if (!RETRYABLE_CODES.has(first.code) || run.stopping) {
      return {
        ok: false,
        code: first.code,
        message: first.message,
        attempts: 1,
      };
    }
    state.stats.retried += 1;
    const second = await attempt(run, state, method, payload, commandId);
    if (second.ok) {
      state.stats.retrySucceeded += 1;
      noteSuccess(state.stats, method);
      return { ok: true, result: second.result, attempts: 2 };
    }
    return {
      ok: false,
      code: second.code,
      message: second.message,
      attempts: 2,
    };
  });
}

/**
 * Sends one `game.say`. With a marker, the send time and expected-observation
 * set feed the event statistics.
 */
export async function sayCommand(
  run: Run,
  state: DrillClient,
  text: string,
  commandId: string,
  marker: string | undefined,
): Promise<boolean> {
  if (marker !== undefined) {
    run.anchorSendTimes.set(marker, nowNs());
  }
  const said = await command(run, state, 'game.say', { text }, commandId);
  if (run.stopping) {
    return false;
  }
  if (!said.ok) {
    fail(
      run,
      `${state.name}: say ${JSON.stringify(text)} failed (${said.code}: ${said.message})`,
    );
    return false;
  }
  if (marker !== undefined) {
    run.anchorExpected.add(marker);
  }
  return true;
}

export async function connectClients(
  url: string,
  runTag: string,
): Promise<DrillClient[]> {
  const states: DrillClient[] = [];
  try {
    for (let index = 0; index < CLIENT_COUNT; index += 1) {
      const name =
        index === 0
          ? `hs-anchor-${runTag}`
          : index === 1
            ? `hs-watch-${runTag}`
            : `hs-client-${index}-${runTag}`;
      // oxlint-disable-next-line eslint/no-await-in-loop -- connection order is the client index; earlier clients stay tracked on failure
      const client = await GameClient.connect({
        url,
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      states.push({
        index,
        name,
        client,
        stats: emptyStats(),
        queue: new SerialQueue(),
        sessionId: '',
        room: 'square',
      });
    }
    await Promise.all(
      states.map(async (state) => {
        const view = readLogin(
          await state.client.call('game.login', { name: state.name }),
        );
        state.sessionId = view.sessionId;
        state.room = view.room;
      }),
    );
    return states;
  } catch (error) {
    await Promise.allSettled(states.map((state) => state.client.close()));
    throw error;
  }
}

/** Logs out and closes every client; returns problems instead of throwing. */
export async function closeClients(
  clients: readonly DrillClient[],
): Promise<string[]> {
  const problems: string[] = [];
  await Promise.allSettled(
    clients.map(async (state) => {
      try {
        await withDeadline(
          state.client.call('game.logout'),
          LOGOUT_TIMEOUT_MS,
          'logout',
        );
      } catch (error) {
        problems.push(`${state.name}: logout failed: ${messageOf(error)}`);
      }
      try {
        await state.client.close();
      } catch (error) {
        problems.push(`${state.name}: close failed: ${messageOf(error)}`);
      }
    }),
  );
  return problems;
}
