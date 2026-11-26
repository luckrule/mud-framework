import { writeFileSync } from 'node:fs';
import { createInterface } from 'node:readline';

import {
  GameClient,
  GameClientError,
} from '../../../examples/game-slice/src/client-lib.ts';
import type { GameEvent } from '../../../examples/game-slice/src/client-lib.ts';

import {
  messageOf,
  sleep,
  withDeadline,
  write,
  writeError,
} from '../support.ts';
import {
  beginSaySequence,
  commitSaySequence,
  observeArrival,
} from './sequence.ts';
import type { SaySequence, SenderArrivals } from './sequence.ts';
import {
  Histogram,
  effectiveThinkMs,
  envIntegerRequired,
  pickCommand,
  readSharedLoadConfig,
  roomForIndex,
} from './support.ts';
import type {
  CommandName,
  CommandResultJson,
  EventResultJson,
  LoadResultJson,
  SharedLoadConfig,
} from './support.ts';

/**
 * One load generator process: it owns `LOAD_GENERATOR_CLIENTS` WebSocket
 * clients for the whole run, so those clients share this event loop and no
 * other process's. The coordinator gives it a room target and result path,
 * waits for the `ready` line, then releases the measurement barrier over
 * stdin. Each client has at most one request in flight.
 *
 * `game-slice` has no separate `who` handler; the interactive client maps
 * `who` to `game.look` and prints the player list. This generator does the
 * same, so the `who` bucket is the `game.look` read path, not a distinct
 * server handler.
 */

const TOKEN_PREFIX = 'mudload';
const GO_TIMEOUT_MS = 300_000;
const EVENT_DRAIN_MS = 500;
const LOGOUT_TIMEOUT_MS = 2000;

/** Directed exit graph of the game slice, used only to find setup paths. */
const ROOM_GRAPH: Readonly<Record<string, Readonly<Record<string, string>>>> = {
  square: { north: 'market', east: 'tavern' },
  market: { south: 'square', east: 'smithy' },
  smithy: { west: 'market' },
  tavern: { west: 'square' },
};

interface GeneratorConfig {
  readonly shared: SharedLoadConfig;
  readonly url: string;
  readonly generator: number;
  readonly clients: number;
  readonly globalOffset: number;
  readonly resultFile: string;
}

function readGeneratorConfig(): GeneratorConfig {
  const shared = readSharedLoadConfig();
  if (shared.externalUrl === undefined) {
    throw new Error('LOAD_URL is required by a load generator');
  }
  const resultFile = process.env.LOAD_GENERATOR_RESULT?.trim();
  if (resultFile === undefined || resultFile.length === 0) {
    throw new Error('LOAD_GENERATOR_RESULT is required by a load generator');
  }
  return {
    shared,
    url: shared.externalUrl,
    generator: envIntegerRequired('LOAD_GENERATOR_INDEX', 0),
    clients: envIntegerRequired('LOAD_GENERATOR_CLIENTS', 1),
    globalOffset: envIntegerRequired('LOAD_GENERATOR_OFFSET', 0),
    resultFile,
  };
}

interface ClientRuntime {
  readonly client: GameClient;
  readonly name: string;
  room: string;
  /** Incremented on every room change; part of the say token. */
  epoch: number;
  /** Next say seq; advances only when the Server accepts the say. */
  readonly saySequence: SaySequence;
  /** Last (epoch, seq) seen per sender, for within-epoch gap detection. */
  readonly seenBySender: Map<string, SenderArrivals>;
}

interface CommandAccumulator {
  count: number;
  errors: number;
  readonly histogram: Histogram;
}

interface EventAccumulator {
  saysAttempted: number;
  saysSent: number;
  saysFailed: number;
  received: number;
  gaps: number;
  duplicates: number;
  staleRoom: number;
  lateEpoch: number;
  unparsed: number;
  readonly histogram: Histogram;
}

interface GeneratorStats {
  readonly commands: Record<CommandName, CommandAccumulator>;
  readonly events: EventAccumulator;
  readonly errors: Record<string, number>;
  readonly rooms: Record<string, number>;
}

/**
 * The measurement window is shared by every client of this process. The event
 * listener is active from the `go` barrier so a warmup say that lands after
 * the window opens is rejected by its token timestamp instead of its arrival.
 */
const measurement = {
  active: false,
  fromMs: 0,
  untilMs: Number.POSITIVE_INFINITY,
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function roomIdOf(result: unknown): string | undefined {
  if (!isRecord(result) || !isRecord(result.room)) {
    return undefined;
  }
  return typeof result.room.id === 'string' ? result.room.id : undefined;
}

function eventTextOf(payload: unknown): string | undefined {
  if (!isRecord(payload) || typeof payload.text !== 'string') {
    return undefined;
  }
  return payload.text;
}

function isDigits(value: string): boolean {
  return /^\d+$/.test(value);
}

interface SayToken {
  readonly runTag: string;
  readonly sender: string;
  readonly epoch: number;
  readonly seq: number;
  readonly sentAtMs: number;
  readonly room: string;
}

function sayTokenOf(text: string): SayToken | undefined {
  if (!text.startsWith(`${TOKEN_PREFIX}|`)) {
    return undefined;
  }
  const parts = text.split('|');
  if (parts.length !== 7) {
    return undefined;
  }
  const [, runTag, sender, epochText, seqText, sentText, room] = parts;
  if (
    runTag === undefined ||
    sender === undefined ||
    epochText === undefined ||
    seqText === undefined ||
    sentText === undefined ||
    room === undefined ||
    !isDigits(epochText) ||
    !isDigits(seqText) ||
    !isDigits(sentText)
  ) {
    return undefined;
  }
  return {
    runTag,
    sender,
    epoch: Number(epochText),
    seq: Number(seqText),
    sentAtMs: Number(sentText),
    room,
  };
}

function handleEvent(
  runtime: ClientRuntime,
  config: SharedLoadConfig,
  stats: GeneratorStats,
  event: GameEvent,
): void {
  if (event.event !== 'room.say') {
    return;
  }
  const text = eventTextOf(event.payload);
  const token = text === undefined ? undefined : sayTokenOf(text);
  if (token === undefined) {
    if (measurement.active) {
      stats.events.unparsed += 1;
    }
    return;
  }
  if (token.runTag !== config.runTag || token.sender === runtime.name) {
    return;
  }
  if (!measurement.active) {
    return;
  }
  if (
    token.sentAtMs < measurement.fromMs ||
    token.sentAtMs > measurement.untilMs
  ) {
    return;
  }
  // The Server emits room.say only to sessions in the sender's room at send
  // time, so every arrival is a same-room delivery even if the receiver moved
  // while the event was queued. Count that move separately but keep the
  // sample: dropping it would bias latency toward the events that outran a
  // receiver's move.
  if (token.room !== runtime.room) {
    stats.events.staleRoom += 1;
  }

  stats.events.received += 1;
  stats.events.histogram.record(Math.max(0, Date.now() - token.sentAtMs) * 1e6);

  if (token.room !== runtime.room) {
    // Gap tracking needs a stable receiver room; a queued event that arrived
    // after the move would otherwise repopulate the map for a room we left.
    return;
  }

  // A successful seq only skips a number when the event itself never arrived;
  // rejected say attempts never consume a seq, so they cannot fake a gap here.
  const arrival = observeArrival(
    runtime.seenBySender,
    token.sender,
    token.epoch,
    token.seq,
  );
  stats.events.gaps += arrival.gap;
  if (arrival.duplicate) {
    stats.events.duplicates += 1;
  }
  if (arrival.lateEpoch) {
    stats.events.lateEpoch += 1;
  }
}

function updateRoom(runtime: ClientRuntime, roomId: string | undefined): void {
  if (roomId === undefined || roomId === runtime.room) {
    return;
  }
  runtime.room = roomId;
  runtime.epoch += 1;
  runtime.seenBySender.clear();
}

function shortestPath(from: string, to: string): readonly string[] | undefined {
  if (from === to) {
    return [];
  }
  const previous = new Map<string, { room: string; direction: string }>();
  const queue: string[] = [from];
  const visited = new Set<string>([from]);
  // Array iteration visits elements appended during the loop, so this is a
  // breadth-first walk without a separate cursor.
  for (const room of queue) {
    const exits = ROOM_GRAPH[room];
    if (exits === undefined) {
      continue;
    }
    for (const [direction, next] of Object.entries(exits)) {
      if (visited.has(next)) {
        continue;
      }
      previous.set(next, { room, direction });
      if (next === to) {
        const path: string[] = [];
        let cursor = to;
        for (;;) {
          const step = previous.get(cursor);
          if (step === undefined) {
            return undefined;
          }
          path.push(step.direction);
          if (step.room === from) {
            return path.toReversed();
          }
          cursor = step.room;
        }
      }
      visited.add(next);
      queue.push(next);
    }
  }
  return undefined;
}

/** Moves within square<->market; smithy and tavern clients first walk out. */
function movementDirection(room: string): string {
  switch (room) {
    case 'square': {
      return 'north';
    }
    case 'market': {
      return 'south';
    }
    case 'smithy': {
      return 'west';
    }
    case 'tavern': {
      return 'west';
    }
    default: {
      return 'north';
    }
  }
}

function newCommandAccumulator(): CommandAccumulator {
  return { count: 0, errors: 0, histogram: new Histogram() };
}

function emptyStats(): GeneratorStats {
  return {
    commands: {
      look: newCommandAccumulator(),
      who: newCommandAccumulator(),
      move: newCommandAccumulator(),
      say: newCommandAccumulator(),
    },
    events: {
      saysAttempted: 0,
      saysSent: 0,
      saysFailed: 0,
      received: 0,
      gaps: 0,
      duplicates: 0,
      staleRoom: 0,
      lateEpoch: 0,
      unparsed: 0,
      histogram: new Histogram(),
    },
    errors: {},
    rooms: {},
  };
}

function recordSuccess(
  stats: GeneratorStats,
  command: CommandName,
  elapsedNs: number,
): void {
  const bucket = stats.commands[command];
  bucket.count += 1;
  bucket.histogram.record(elapsedNs);
}

function recordError(
  stats: GeneratorStats,
  command: CommandName,
  error: unknown,
): void {
  const code = error instanceof GameClientError ? error.code : 'UNKNOWN';
  stats.errors[code] = (stats.errors[code] ?? 0) + 1;
  stats.commands[command].errors += 1;
}

function isClosedError(error: unknown): boolean {
  return (
    error instanceof GameClientError &&
    (error.code === 'CLIENT_CLOSED' || error.code === 'CLIENT_SEND_FAILED')
  );
}

async function setupClient(
  config: GeneratorConfig,
  index: number,
  stats: GeneratorStats,
): Promise<ClientRuntime> {
  const name = `load-${config.generator}-${index}`;
  const client = await GameClient.connect({
    url: config.url,
    timeoutMs: config.shared.requestTimeoutMs,
  });
  const runtime: ClientRuntime = {
    client,
    name,
    room: '',
    epoch: 0,
    saySequence: { nextSeq: 0 },
    seenBySender: new Map(),
  };
  client.onEvent((event) => {
    handleEvent(runtime, config.shared, stats, event);
  });
  try {
    const login = await client.call('game.login', { name });
    updateRoom(runtime, roomIdOf(login));
    if (runtime.room.length === 0) {
      throw new Error(`login for ${name} returned no room id`);
    }
    const target = roomForIndex(
      config.globalOffset + index,
      config.shared.roomSize,
    );
    const path = shortestPath(runtime.room, target);
    if (path === undefined) {
      throw new Error(`no exit path from ${runtime.room} to ${target}`);
    }
    for (const direction of path) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- setup walks one exit at a time for this client
      const moved = await client.call('game.move', { direction });
      updateRoom(runtime, roomIdOf(moved));
    }
    if (runtime.room !== target) {
      throw new Error(
        `${name} ended setup in ${runtime.room}, expected ${target}`,
      );
    }
    return runtime;
  } catch (error) {
    await client.close();
    throw error;
  }
}

type CommandOutcome = 'ok' | 'closed';

async function executeCommand(
  runtime: ClientRuntime,
  shared: SharedLoadConfig,
  stats: GeneratorStats,
  command: CommandName,
  measured: boolean,
): Promise<CommandOutcome> {
  const startedNs = Bun.nanoseconds();
  try {
    switch (command) {
      case 'look':
      case 'who': {
        const result = await runtime.client.call('game.look');
        updateRoom(runtime, roomIdOf(result));
        break;
      }
      case 'move': {
        const result = await runtime.client.call('game.move', {
          direction: movementDirection(runtime.room),
        });
        updateRoom(runtime, roomIdOf(result));
        break;
      }
      case 'say': {
        const sentAtMs = Date.now();
        // Reserve without advancing: a rejected attempt must leave no seq hole
        // for the receiver, which only sees the accepted (broadcast) says.
        const seq = beginSaySequence(runtime.saySequence);
        if (measured) {
          stats.events.saysAttempted += 1;
        }
        const text = `${TOKEN_PREFIX}|${shared.runTag}|${runtime.name}|${runtime.epoch}|${seq}|${sentAtMs}|${runtime.room}`;
        await runtime.client.call('game.say', { text });
        commitSaySequence(runtime.saySequence, seq);
        if (measured) {
          stats.events.saysSent += 1;
        }
        break;
      }
      default: {
        const unreachable: never = command;
        throw new Error(`unhandled command ${String(unreachable)}`);
      }
    }
    if (measured) {
      recordSuccess(stats, command, Bun.nanoseconds() - startedNs);
    }
    return 'ok';
  } catch (error) {
    if (measured) {
      recordError(stats, command, error);
      if (command === 'say') {
        stats.events.saysFailed += 1;
      }
    }
    return isClosedError(error) ? 'closed' : 'ok';
  }
}

async function clientLoop(
  runtime: ClientRuntime,
  config: GeneratorConfig,
  stats: GeneratorStats,
  endsAtMs: number,
  thinkMs: number,
): Promise<void> {
  for (;;) {
    const issueAtMs = Date.now();
    if (issueAtMs >= endsAtMs) {
      return;
    }
    const measured = issueAtMs >= measurement.fromMs;
    const command = pickCommand(config.shared.mix);
    // oxlint-disable-next-line eslint/no-await-in-loop -- one in-flight request per client is the measured model
    const outcome = await executeCommand(
      runtime,
      config.shared,
      stats,
      command,
      measured,
    );
    if (outcome === 'closed') {
      return;
    }
    if (thinkMs > 0) {
      const delay = Math.round(thinkMs * (0.5 + Math.random()));
      // oxlint-disable-next-line eslint/no-await-in-loop -- the pause is the simulated player think time
      await sleep(delay);
    }
  }
}

function waitForGo(): Promise<void> {
  const go = Promise.withResolvers<void>();
  const reader = createInterface({ input: process.stdin, terminal: false });
  reader.on('line', (line) => {
    if (line.trim() === 'go') {
      go.resolve();
    }
  });
  reader.on('close', () => {
    go.reject(new Error('stdin closed before the go barrier'));
  });
  return withDeadline(go.promise, GO_TIMEOUT_MS, 'go barrier').finally(() => {
    reader.close();
  });
}

async function shutdownClients(
  clients: readonly ClientRuntime[],
): Promise<void> {
  await Promise.allSettled(
    clients.map((runtime) =>
      withDeadline(
        runtime.client.call('game.logout'),
        LOGOUT_TIMEOUT_MS,
        'load logout',
      ).catch(() => undefined),
    ),
  );
  await Promise.allSettled(clients.map((runtime) => runtime.client.close()));
}

function commandJson(accumulator: CommandAccumulator): CommandResultJson {
  return {
    count: accumulator.count,
    errors: accumulator.errors,
    histogram: accumulator.histogram.toJson(),
  };
}

function eventJson(accumulator: EventAccumulator): EventResultJson {
  return {
    saysAttempted: accumulator.saysAttempted,
    saysSent: accumulator.saysSent,
    saysFailed: accumulator.saysFailed,
    received: accumulator.received,
    gaps: accumulator.gaps,
    duplicates: accumulator.duplicates,
    staleRoom: accumulator.staleRoom,
    lateEpoch: accumulator.lateEpoch,
    unparsed: accumulator.unparsed,
    histogram: accumulator.histogram.toJson(),
  };
}

function resultJson(
  config: GeneratorConfig,
  stats: GeneratorStats,
  setupMs: number,
  startedAtMs: number,
  measuredFromMs: number,
  measuredUntilMs: number,
): LoadResultJson {
  return {
    version: 1,
    generator: config.generator,
    clients: config.clients,
    setupMs,
    startedAtMs,
    measuredFromMs,
    measuredUntilMs,
    commands: {
      look: commandJson(stats.commands.look),
      who: commandJson(stats.commands.who),
      move: commandJson(stats.commands.move),
      say: commandJson(stats.commands.say),
    },
    events: eventJson(stats.events),
    errors: { ...stats.errors },
    rooms: { ...stats.rooms },
  };
}

async function runGenerator(config: GeneratorConfig): Promise<void> {
  const stats = emptyStats();
  const clients: ClientRuntime[] = [];
  const setupStartedMs = Date.now();
  let setupMs = 0;
  let startedAtMs = 0;
  let measuredFromMs = 0;
  let measuredUntilMs = 0;
  try {
    for (let index = 0; index < config.clients; index += 1) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- one login and setup walk per client, deliberately serial
      const runtime = await setupClient(config, index, stats);
      clients.push(runtime);
    }
    setupMs = Date.now() - setupStartedMs;
    write('ready');

    // The barrier is the single release point for measurement.
    await waitForGo();
    startedAtMs = Date.now();
    measuredFromMs = startedAtMs + config.shared.warmupSec * 1000;
    measuredUntilMs = measuredFromMs + config.shared.durationSec * 1000;
    measurement.fromMs = measuredFromMs;
    measurement.untilMs = measuredUntilMs;
    measurement.active = true;

    await Promise.all(
      clients.map((runtime) =>
        clientLoop(
          runtime,
          config,
          stats,
          measuredUntilMs,
          effectiveThinkMs(config.shared),
        ),
      ),
    );
    // The window is the nominal duration: trailing think time and a last
    // in-flight request may finish after it but must not stretch the
    // denominator. Tokens are filtered by send timestamp, so the drain below
    // only lets already-sent events land.
    await sleep(EVENT_DRAIN_MS);
    measurement.active = false;
    for (const runtime of clients) {
      stats.rooms[runtime.room] = (stats.rooms[runtime.room] ?? 0) + 1;
    }
  } finally {
    await shutdownClients(clients);
  }

  writeFileSync(
    config.resultFile,
    JSON.stringify(
      resultJson(
        config,
        stats,
        setupMs,
        startedAtMs,
        measuredFromMs,
        measuredUntilMs,
      ),
    ),
  );
  write('done');
}

try {
  const config = readGeneratorConfig();
  await runGenerator(config);
} catch (error) {
  writeError(`load generator failed: ${messageOf(error)}`);
  process.exitCode = 1;
}

if (process.exitCode !== undefined && process.exitCode !== 0) {
  // oxlint-disable-next-line unicorn/no-process-exit -- a failed generator must not hang on sockets it could not close
  process.exit(1);
}
// A closed WebSocket can still hold a Bun handle; the walk is complete, so do
// not let the process linger after the coordinator has its result file.
// oxlint-disable-next-line unicorn/no-process-exit -- explicit success exit after the done signal
process.exit(0);
