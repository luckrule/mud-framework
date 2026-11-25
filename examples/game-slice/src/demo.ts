import { GameClient } from './client-lib.ts';
import type { GameEvent } from './client-lib.ts';
import type { ItemView, RoomSnapshot } from './world.ts';

/**
 * Scripted two-player demo. It connects to an already running Portal, drives
 * two clients through the core slice (login, movement, room broadcast, item
 * pickup) and fails with a non-zero exit code if any expectation breaks.
 * `GAME_SLICE_URL` or `PORTAL_PORT` selects the Portal; it does not spawn one.
 */
const DEFAULT_PORTAL_PORT = 8080;
const EVENT_TIMEOUT_MS = 5000;

interface LoginResult {
  readonly sessionId: string;
  readonly room: RoomSnapshot;
}

interface MoveResult {
  readonly moved: boolean;
  readonly replayed?: boolean;
  readonly room: RoomSnapshot;
}

interface TakeResult {
  readonly item: ItemView;
  readonly alreadyOwned: boolean;
}

interface InventoryResult {
  readonly items: ItemView[];
}

function write(line: string): void {
  process.stdout.write(`${line}\n`);
}

function writeError(line: string): void {
  process.stderr.write(`${line}\n`);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

function payloadOf(event: GameEvent): Record<string, unknown> {
  if (!isRecord(event.payload)) {
    throw new Error(`${event.event} payload is not an object`);
  }
  return event.payload;
}

function readPort(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > 65535) {
    throw new Error(
      `${name} must be an integer between 1 and 65535, got ${JSON.stringify(raw)}`,
    );
  }
  return value;
}

function resolveUrl(): string {
  const explicit = process.env.GAME_SLICE_URL;
  if (explicit !== undefined && explicit.length > 0) {
    return explicit;
  }
  return `ws://127.0.0.1:${readPort('PORTAL_PORT', DEFAULT_PORTAL_PORT)}/`;
}

function step(label: string): void {
  write(`demo: ${label}`);
}

async function main(): Promise<void> {
  const url = resolveUrl();
  write(`demo: connecting to ${url}`);
  const alice = await GameClient.connect({ url });
  const bob = await GameClient.connect({ url });
  try {
    step('alice logs in (starts in the square)');
    const aliceLogin = await alice.call<LoginResult>('game.login', {
      name: 'alice',
    });
    assert(
      aliceLogin.sessionId === 'player:alice',
      `expected session player:alice, got ${aliceLogin.sessionId}`,
    );
    assert(
      aliceLogin.room.id === 'square',
      `expected alice in square, got ${aliceLogin.room.id}`,
    );

    step('bob logs in (also in the square)');
    const bobLogin = await bob.call<LoginResult>('game.login', {
      name: 'bob',
    });
    assert(
      bobLogin.room.id === 'square',
      `expected bob in square, got ${bobLogin.room.id}`,
    );

    step('alice says hello; bob receives room.say');
    const bobSawSay = bob.nextEvent(
      (event) => event.event === 'room.say',
      EVENT_TIMEOUT_MS,
    );
    const sayResult = await alice.call<{ said: boolean }>('game.say', {
      text: 'hello bob',
    });
    assert(sayResult.said, 'game.say did not report success');
    const sayEvent = await bobSawSay;
    const sayPayload = payloadOf(sayEvent);
    assert(
      sayPayload.from === 'alice' && sayPayload.text === 'hello bob',
      `unexpected room.say payload: ${JSON.stringify(sayEvent.payload)}`,
    );

    step('alice moves north; bob receives room.left');
    const bobSawLeave = bob.nextEvent(
      (event) => event.event === 'room.left',
      EVENT_TIMEOUT_MS,
    );
    const aliceMoved = await alice.call<MoveResult>('game.move', {
      direction: 'north',
    });
    assert(
      aliceMoved.room.id === 'market',
      `expected alice in market, got ${aliceMoved.room.id}`,
    );
    const leftPayload = payloadOf(await bobSawLeave);
    assert(
      leftPayload.player === 'alice' &&
        leftPayload.direction === 'north' &&
        leftPayload.from === 'square' &&
        leftPayload.to === 'market',
      `unexpected room.left payload: ${JSON.stringify(leftPayload)}`,
    );

    step('bob moves north; alice receives room.entered');
    const aliceSawEnter = alice.nextEvent(
      (event) => event.event === 'room.entered',
      EVENT_TIMEOUT_MS,
    );
    const bobMoved = await bob.call<MoveResult>('game.move', {
      direction: 'north',
    });
    assert(
      bobMoved.room.id === 'market',
      `expected bob in market, got ${bobMoved.room.id}`,
    );
    const enteredPayload = payloadOf(await aliceSawEnter);
    assert(
      enteredPayload.player === 'bob' &&
        enteredPayload.from === 'square' &&
        enteredPayload.to === 'market',
      `unexpected room.entered payload: ${JSON.stringify(enteredPayload)}`,
    );

    step('alice takes the bread; bob receives item.taken');
    const bobSawTake = bob.nextEvent(
      (event) => event.event === 'item.taken',
      EVENT_TIMEOUT_MS,
    );
    const takeResult = await alice.call<TakeResult>('game.take', {
      item: 'bread',
    });
    assert(
      takeResult.item.id === 'bread' && !takeResult.alreadyOwned,
      `unexpected take result: ${JSON.stringify(takeResult)}`,
    );
    const takePayload = payloadOf(await bobSawTake);
    assert(
      takePayload.player === 'alice' &&
        takePayload.room === 'market' &&
        isRecord(takePayload.item) &&
        takePayload.item.id === 'bread',
      `unexpected item.taken payload: ${JSON.stringify(takePayload)}`,
    );

    step('alice inventory contains the bread');
    const inventory = await alice.call<InventoryResult>('game.inventory');
    assert(
      inventory.items.some((item) => item.id === 'bread'),
      `inventory is missing the bread: ${JSON.stringify(inventory.items)}`,
    );

    step('alice moves east; bob receives room.left');
    const bobSawLeaveAgain = bob.nextEvent(
      (event) => event.event === 'room.left',
      EVENT_TIMEOUT_MS,
    );
    const smithy = await alice.call<MoveResult>('game.move', {
      direction: 'east',
    });
    assert(
      smithy.room.id === 'smithy',
      `expected alice in smithy, got ${smithy.room.id}`,
    );
    const leaveAgain = payloadOf(await bobSawLeaveAgain);
    assert(
      leaveAgain.player === 'alice' &&
        leaveAgain.from === 'market' &&
        leaveAgain.to === 'smithy',
      `unexpected second room.left payload: ${JSON.stringify(leaveAgain)}`,
    );

    step('alice looks around the smithy');
    const look = await alice.call<{ room: RoomSnapshot }>('game.look');
    assert(
      look.room.id === 'smithy' && look.room.exits.includes('west'),
      `unexpected smithy snapshot: ${JSON.stringify(look.room)}`,
    );
  } finally {
    await Promise.allSettled([
      alice.call('game.logout'),
      bob.call('game.logout'),
    ]);
    await alice.close();
    await bob.close();
  }
  write('demo: all assertions passed');
}

try {
  await main();
} catch (error) {
  writeError(`demo failed: ${messageOf(error)}`);
  process.exitCode = 1;
}
