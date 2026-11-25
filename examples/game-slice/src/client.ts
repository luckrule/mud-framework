import { createInterface } from 'node:readline';

import { GameClient } from './client-lib.ts';
import type { GameEvent } from './client-lib.ts';
import type { ItemView, RoomSnapshot } from './world.ts';

/**
 * Interactive console for the game slice. Login is by name only; this is a
 * demo identity rule, not authentication.
 */
const DEFAULT_PORTAL_PORT = 8080;

interface LoginResult {
  readonly sessionId: string;
  readonly room: RoomSnapshot;
}

interface LookResult {
  readonly room: RoomSnapshot;
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

function show(value: unknown): string {
  return JSON.stringify(value) ?? String(value);
}

function textField(payload: unknown, field: string): string {
  const value = isRecord(payload) ? payload[field] : undefined;
  return typeof value === 'string' ? value : '?';
}

function itemLabel(payload: unknown): string {
  const item = isRecord(payload) ? payload.item : undefined;
  if (!isRecord(item)) {
    return 'an item';
  }
  const name = item.name;
  if (typeof name === 'string') {
    return name;
  }
  const id = item.id;
  return typeof id === 'string' ? id : 'an item';
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

function readPlayerName(): string {
  const flag = process.argv.indexOf('--name');
  const fromArgv = flag !== -1 ? process.argv[flag + 1] : undefined;
  const name = fromArgv ?? process.env.PLAYER_NAME;
  if (name === undefined || name.trim().length === 0) {
    throw new Error('pass --name <player> or set PLAYER_NAME');
  }
  return name.trim();
}

function printRoom(room: RoomSnapshot): void {
  write(`== ${room.name} (${room.id}) ==`);
  write(room.description);
  write(`Exits: ${room.exits.length > 0 ? room.exits.join(', ') : 'none'}`);
  write(
    `Items: ${
      room.items.length > 0
        ? room.items.map((item) => `${item.id} (${item.name})`).join(', ')
        : 'none'
    }`,
  );
  write(
    `Players here: ${
      room.players.length > 0 ? room.players.join(', ') : 'nobody else'
    }`,
  );
}

function formatEvent(event: GameEvent): string {
  const payload: unknown = event.payload;
  switch (event.event) {
    case 'room.say':
      return `[say] ${textField(payload, 'from')}: ${textField(payload, 'text')}`;
    case 'room.left':
      return `[left] ${textField(payload, 'player')} went ${textField(payload, 'direction')} (${textField(payload, 'from')} -> ${textField(payload, 'to')})`;
    case 'room.entered':
      return `[entered] ${textField(payload, 'player')} arrived from ${textField(payload, 'from')}`;
    case 'item.taken':
      return `[taken] ${textField(payload, 'player')} picked up ${itemLabel(payload)}`;
    case 'item.dropped':
      return `[dropped] ${textField(payload, 'player')} dropped ${itemLabel(payload)}`;
    default:
      return `[${event.event}] ${show(payload)}`;
  }
}

function requireArgument(argument: string, usage: string): string {
  if (argument.length === 0) {
    throw new Error(`usage: ${usage}`);
  }
  return argument;
}

async function runCommand(
  client: GameClient,
  line: string,
): Promise<'continue' | 'quit'> {
  const trimmed = line.trim();
  if (trimmed.length === 0) {
    return 'continue';
  }
  const [verbRaw, ...rest] = trimmed.split(/\s+/);
  const verb = (verbRaw ?? '').toLowerCase();
  const argument = rest.join(' ');

  switch (verb) {
    case 'look':
    case 'l': {
      const result = await client.call<LookResult>('game.look');
      printRoom(result.room);
      return 'continue';
    }
    case 'who': {
      const result = await client.call<LookResult>('game.look');
      const players = result.room.players;
      write(
        `Players here: ${
          players.length > 0 ? players.join(', ') : 'nobody else'
        }`,
      );
      return 'continue';
    }
    case 'go': {
      const direction = requireArgument(argument, 'go <direction>');
      const result = await client.call<{ room: RoomSnapshot }>('game.move', {
        direction,
      });
      printRoom(result.room);
      return 'continue';
    }
    case 'say': {
      const text = requireArgument(argument, 'say <text>');
      await client.call('game.say', { text });
      write('You say it.');
      return 'continue';
    }
    case 'take': {
      const item = requireArgument(argument, 'take <item>');
      const result = await client.call<{
        item: ItemView;
        alreadyOwned: boolean;
      }>('game.take', { item });
      write(
        result.alreadyOwned
          ? `You already have ${result.item.name}.`
          : `You take ${result.item.name}.`,
      );
      return 'continue';
    }
    case 'drop': {
      const item = requireArgument(argument, 'drop <item>');
      const result = await client.call<{
        item: ItemView;
        alreadyHere: boolean;
      }>('game.drop', { item });
      write(
        result.alreadyHere
          ? `${result.item.name} is already here.`
          : `You drop ${result.item.name}.`,
      );
      return 'continue';
    }
    case 'inv':
    case 'inventory': {
      const result = await client.call<InventoryResult>('game.inventory');
      write(
        result.items.length > 0
          ? `You carry: ${result.items.map((item) => `${item.id} (${item.name})`).join(', ')}`
          : 'You carry nothing.',
      );
      return 'continue';
    }
    case 'quit':
    case 'exit':
      return 'quit';
    default:
      writeError(
        `unknown command "${verb}"; try look, go <dir>, say <text>, take <item>, drop <item>, inv, who, quit`,
      );
      return 'continue';
  }
}

async function main(): Promise<void> {
  const name = readPlayerName();
  const url = resolveUrl();
  const client = await GameClient.connect({ url });
  client.onEvent((event) => {
    write(formatEvent(event));
  });
  try {
    const login = await client.call<LoginResult>('game.login', { name });
    write(`logged in as ${name} (${login.sessionId})`);
    printRoom(login.room);

    const rl = createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: process.stdin.isTTY,
    });
    rl.setPrompt('> ');

    // A promise chain rather than `for await (const line of rl)`: with piped
    // stdin the interface reaches EOF while a command is still awaiting its
    // reply, and Bun's async iterator reports that as an error instead of
    // letting the remaining lines drain.
    let chain = Promise.resolve();
    let quit = false;
    let closed = false;
    rl.on('line', (line) => {
      chain = chain.then(async () => {
        if (quit) {
          return undefined;
        }
        try {
          if ((await runCommand(client, line)) === 'quit') {
            quit = true;
            rl.close();
            return undefined;
          }
        } catch (error) {
          writeError(`error: ${messageOf(error)}`);
        }
        // `prompt()` on an interface already closed by EOF throws.
        if (!closed) {
          rl.prompt();
        }
        return undefined;
      });
    });
    const inputClosed = new Promise<void>((resolve) => {
      rl.once('close', () => {
        closed = true;
        resolve();
      });
    });
    rl.prompt();
    await inputClosed;
    await chain;
    await client.call('game.logout').catch(() => undefined);
  } finally {
    await client.close();
  }
  write('bye');
}

try {
  await main();
} catch (error) {
  writeError(`client failed: ${messageOf(error)}`);
  process.exitCode = 1;
}
