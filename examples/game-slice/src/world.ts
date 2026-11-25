import { BusinessError } from 'mud-framework-core';
import type { StorageBackend, StorageTransaction } from 'mud-framework-core';

/**
 * The game slice's data layer: schema, seed and the SQL the handlers use.
 * Nothing here registers a Handler or touches the wire.
 *
 * `applied_moves` is the business idempotency ledger for movement. The
 * framework may replay a request after a Server crash, with the same
 * `requestId`; recording the move under that id lets `game.move` tell a replay
 * from a fresh command, so the player is never moved twice.
 */
export const START_ROOM = 'square';
export const MAX_NAME_LENGTH = 32;
export const MAX_SAY_LENGTH = 500;
export const MAX_ITEM_TERM_LENGTH = 64;

export interface RoomRow {
  readonly id: string;
  readonly name: string;
  readonly description: string;
}

export interface PlayerRow {
  readonly name: string;
  readonly room_id: string;
  readonly session_id: string | null;
  /** Framework Connection id of the login that currently owns this Session. */
  readonly holder_connection_id: string | null;
}

export interface ItemRow {
  readonly id: string;
  readonly name: string;
  readonly room_id: string | null;
  readonly owner: string | null;
}

export interface ItemView {
  readonly id: string;
  readonly name: string;
}

export interface RoomSnapshot {
  readonly id: string;
  readonly name: string;
  readonly description: string;
  readonly exits: string[];
  readonly items: ItemView[];
  /** Other online players in the room, ordered by name. */
  readonly players: string[];
  readonly you: string;
}

export interface AppliedMove {
  readonly player: string;
  readonly from_room: string;
  readonly to_room: string;
  readonly direction: string;
}

interface SeedRoom {
  readonly id: string;
  readonly name: string;
  readonly description: string;
}

interface SeedExit {
  readonly roomId: string;
  readonly direction: string;
  readonly toRoom: string;
}

interface SeedItem {
  readonly id: string;
  readonly name: string;
  readonly roomId: string;
}

const SEED_ROOMS: readonly SeedRoom[] = [
  {
    id: 'square',
    name: 'Town Square',
    description:
      'A cracked fountain stands at the centre of a small square. Cold wind carries the smell of bread from the north, and light spills from a doorway to the east.',
  },
  {
    id: 'market',
    name: 'Old Market',
    description:
      'Empty stalls line the cobbles. A narrow lane leads north into the dark, and a forge glows to the east.',
  },
  {
    id: 'smithy',
    name: 'The Smithy',
    description:
      'A cold anvil and a wall of rusted tools. The only way out is west, back to the market.',
  },
  {
    id: 'tavern',
    name: 'The Broken Flagon',
    description:
      'A low room of scarred tables and a dead hearth. The square lies west, through a creaking door.',
  },
];

const SEED_EXITS: readonly SeedExit[] = [
  { roomId: 'square', direction: 'north', toRoom: 'market' },
  { roomId: 'square', direction: 'east', toRoom: 'tavern' },
  { roomId: 'market', direction: 'south', toRoom: 'square' },
  { roomId: 'market', direction: 'east', toRoom: 'smithy' },
  { roomId: 'smithy', direction: 'west', toRoom: 'market' },
  { roomId: 'tavern', direction: 'west', toRoom: 'square' },
];

const SEED_ITEMS: readonly SeedItem[] = [
  { id: 'sword', name: 'rusty sword', roomId: 'square' },
  { id: 'bread', name: 'loaf of bread', roomId: 'market' },
  { id: 'lantern', name: 'brass lantern', roomId: 'tavern' },
];

export async function initializeWorld(backend: StorageBackend): Promise<void> {
  await backend.transaction(async (tx) => {
    await createSchema(tx);
    await seedWorld(tx);
  });
}

async function createSchema(tx: StorageTransaction): Promise<void> {
  await tx.execute(
    `CREATE TABLE IF NOT EXISTS rooms (
       id TEXT PRIMARY KEY,
       name TEXT NOT NULL,
       description TEXT NOT NULL
     )`,
  );
  await tx.execute(
    `CREATE TABLE IF NOT EXISTS exits (
       room_id TEXT NOT NULL REFERENCES rooms(id),
       direction TEXT NOT NULL,
       to_room TEXT NOT NULL REFERENCES rooms(id),
       PRIMARY KEY (room_id, direction)
     )`,
  );
  await tx.execute(
    `CREATE TABLE IF NOT EXISTS players (
       name TEXT PRIMARY KEY,
       room_id TEXT NOT NULL REFERENCES rooms(id),
       session_id TEXT,
       holder_connection_id TEXT
     )`,
  );
  await tx.execute(
    `CREATE TABLE IF NOT EXISTS items (
       id TEXT PRIMARY KEY,
       name TEXT NOT NULL,
       room_id TEXT REFERENCES rooms(id),
       owner TEXT REFERENCES players(name)
     )`,
  );
  await tx.execute(
    `CREATE TABLE IF NOT EXISTS applied_moves (
       request_id TEXT PRIMARY KEY,
       player TEXT NOT NULL,
       from_room TEXT NOT NULL,
       to_room TEXT NOT NULL,
       direction TEXT NOT NULL
     )`,
  );
}

/**
 * Seeds are insert-only: a seeded row that already exists keeps its current
 * state, so restarting the Server must not teleport items or overwrite a
 * player's room.
 */
async function seedWorld(tx: StorageTransaction): Promise<void> {
  const roomSlots = SEED_ROOMS.map(() => '(?, ?, ?)').join(', ');
  await tx.execute(
    `INSERT INTO rooms (id, name, description) VALUES ${roomSlots}
     ON CONFLICT(id) DO NOTHING`,
    SEED_ROOMS.flatMap((room) => [room.id, room.name, room.description]),
  );

  const exitSlots = SEED_EXITS.map(() => '(?, ?, ?)').join(', ');
  await tx.execute(
    `INSERT INTO exits (room_id, direction, to_room) VALUES ${exitSlots}
     ON CONFLICT(room_id, direction) DO NOTHING`,
    SEED_EXITS.flatMap((exit) => [exit.roomId, exit.direction, exit.toRoom]),
  );

  const itemSlots = SEED_ITEMS.map(() => '(?, ?, ?, NULL)').join(', ');
  await tx.execute(
    `INSERT INTO items (id, name, room_id, owner) VALUES ${itemSlots}
     ON CONFLICT(id) DO NOTHING`,
    SEED_ITEMS.flatMap((item) => [item.id, item.name, item.roomId]),
  );
}

export async function upsertPlayerLogin(
  tx: StorageTransaction,
  name: string,
  sessionId: string,
  startRoom: string,
  holderConnectionId: string,
): Promise<PlayerRow> {
  await tx.execute(
    `INSERT INTO players (name, room_id, session_id, holder_connection_id)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(name) DO UPDATE SET
       session_id = excluded.session_id,
       holder_connection_id = excluded.holder_connection_id`,
    [name, startRoom, sessionId, holderConnectionId],
  );
  const rows = await tx.query<PlayerRow>(
    `SELECT name, room_id, session_id, holder_connection_id
     FROM players WHERE name = ?`,
    [name],
  );
  const player = rows[0];
  if (player === undefined) {
    throw new Error(`player "${name}" disappeared after upsert`);
  }
  return player;
}

export async function findPlayerByName(
  tx: StorageTransaction,
  name: string,
): Promise<PlayerRow | undefined> {
  const rows = await tx.query<PlayerRow>(
    `SELECT name, room_id, session_id, holder_connection_id
     FROM players WHERE name = ?`,
    [name],
  );
  return rows[0];
}

export async function findPlayerBySession(
  tx: StorageTransaction,
  sessionId: string,
): Promise<PlayerRow | undefined> {
  const rows = await tx.query<PlayerRow>(
    `SELECT name, room_id, session_id, holder_connection_id
     FROM players WHERE session_id = ?`,
    [sessionId],
  );
  return rows[0];
}

export async function setPlayerRoom(
  tx: StorageTransaction,
  name: string,
  roomId: string,
): Promise<void> {
  await tx.execute('UPDATE players SET room_id = ? WHERE name = ?', [
    roomId,
    name,
  ]);
}

/**
 * Clears the online marker only when `connectionId` is still the holder.
 * A logout that entered before a same-name takeover can commit after the new
 * login; without this guard it would erase the new holder's marker and every
 * later session request would fail with `UNKNOWN_PLAYER`.
 */
export async function clearPlayerSessionIfHolder(
  tx: StorageTransaction,
  name: string,
  connectionId: string,
): Promise<boolean> {
  const rows = await tx.query<{ holder_connection_id: string | null }>(
    'SELECT holder_connection_id FROM players WHERE name = ?',
    [name],
  );
  if (rows[0]?.holder_connection_id !== connectionId) {
    return false;
  }
  await tx.execute(
    `UPDATE players SET session_id = NULL, holder_connection_id = NULL
     WHERE name = ? AND holder_connection_id = ?`,
    [name, connectionId],
  );
  return true;
}

export async function findExit(
  tx: StorageTransaction,
  roomId: string,
  direction: string,
): Promise<string | undefined> {
  const rows = await tx.query<{ to_room: string }>(
    'SELECT to_room FROM exits WHERE room_id = ? AND direction = ?',
    [roomId, direction],
  );
  return rows[0]?.to_room;
}

/**
 * Sessions of the other players currently in `roomId`. A player row with a
 * NULL `session_id` is offline (logged out) and is skipped; so is the mover.
 */
export async function listOtherSessions(
  tx: StorageTransaction,
  roomId: string,
  excludeName: string,
): Promise<string[]> {
  const rows = await tx.query<{ session_id: string }>(
    `SELECT session_id FROM players
     WHERE room_id = ? AND session_id IS NOT NULL AND name <> ?
     ORDER BY name`,
    [roomId, excludeName],
  );
  return rows.map((row) => row.session_id);
}

export async function roomSnapshot(
  tx: StorageTransaction,
  roomId: string,
  you: string,
): Promise<RoomSnapshot> {
  const rooms = await tx.query<RoomRow>(
    'SELECT id, name, description FROM rooms WHERE id = ?',
    [roomId],
  );
  const room = rooms[0];
  if (room === undefined) {
    throw new BusinessError('NO_SUCH_ROOM', `room "${roomId}" does not exist`);
  }
  const exits = await tx.query<{ direction: string }>(
    'SELECT direction FROM exits WHERE room_id = ? ORDER BY direction',
    [roomId],
  );
  const items = await tx.query<ItemView>(
    `SELECT id, name FROM items
     WHERE room_id = ? AND owner IS NULL
     ORDER BY id`,
    [roomId],
  );
  const players = await tx.query<{ name: string }>(
    `SELECT name FROM players
     WHERE room_id = ? AND session_id IS NOT NULL AND name <> ?
     ORDER BY name`,
    [roomId, you],
  );
  return {
    id: room.id,
    name: room.name,
    description: room.description,
    exits: exits.map((row) => row.direction),
    items,
    players: players.map((row) => row.name),
    you,
  };
}

export async function listInventory(
  tx: StorageTransaction,
  owner: string,
): Promise<ItemView[]> {
  return tx.query<ItemView>(
    'SELECT id, name FROM items WHERE owner = ? ORDER BY id',
    [owner],
  );
}

/** An item on the floor of `roomId`, matched by id or display name. */
export async function findItemOnFloor(
  tx: StorageTransaction,
  roomId: string,
  term: string,
): Promise<ItemRow | undefined> {
  const rows = await tx.query<ItemRow>(
    `SELECT id, name, room_id, owner FROM items
     WHERE room_id = ? AND owner IS NULL AND (id = ? OR name = ?)
     ORDER BY id
     LIMIT 1`,
    [roomId, term, term],
  );
  return rows[0];
}

/** An item in `owner`'s inventory, matched by id or display name. */
export async function findItemOwnedBy(
  tx: StorageTransaction,
  owner: string,
  term: string,
): Promise<ItemRow | undefined> {
  const rows = await tx.query<ItemRow>(
    `SELECT id, name, room_id, owner FROM items
     WHERE owner = ? AND (id = ? OR name = ?)
     ORDER BY id
     LIMIT 1`,
    [owner, term, term],
  );
  return rows[0];
}

/** Any item with this id or name, wherever it is. */
export async function findItemByTerm(
  tx: StorageTransaction,
  term: string,
): Promise<ItemRow | undefined> {
  const rows = await tx.query<ItemRow>(
    `SELECT id, name, room_id, owner FROM items
     WHERE id = ? OR name = ?
     ORDER BY id
     LIMIT 1`,
    [term, term],
  );
  return rows[0];
}

/**
 * Conditional take: only an unowned item in the player's room can move into
 * their inventory. Returns whether this call is the one that moved it.
 */
export async function takeItem(
  tx: StorageTransaction,
  itemId: string,
  player: string,
  roomId: string,
): Promise<boolean> {
  await tx.execute(
    `UPDATE items SET owner = ?, room_id = NULL
     WHERE id = ? AND owner IS NULL AND room_id = ?`,
    [player, itemId, roomId],
  );
  const rows = await tx.query<{ owner: string | null }>(
    'SELECT owner FROM items WHERE id = ?',
    [itemId],
  );
  return rows[0]?.owner === player;
}

/**
 * Conditional drop: only an item the player owns can go back on the floor.
 * Returns whether this call is the one that moved it.
 */
export async function dropItem(
  tx: StorageTransaction,
  itemId: string,
  player: string,
  roomId: string,
): Promise<boolean> {
  await tx.execute(
    'UPDATE items SET owner = NULL, room_id = ? WHERE id = ? AND owner = ?',
    [roomId, itemId, player],
  );
  const rows = await tx.query<{ owner: string | null; room_id: string | null }>(
    'SELECT owner, room_id FROM items WHERE id = ?',
    [itemId],
  );
  const item = rows[0];
  if (item === undefined) {
    return false;
  }
  return item.owner === null && item.room_id === roomId;
}

export async function findAppliedMove(
  tx: StorageTransaction,
  requestId: string,
): Promise<AppliedMove | undefined> {
  const rows = await tx.query<AppliedMove>(
    `SELECT player, from_room, to_room, direction FROM applied_moves
     WHERE request_id = ?`,
    [requestId],
  );
  return rows[0];
}

export async function recordMove(
  tx: StorageTransaction,
  requestId: string,
  move: AppliedMove,
): Promise<void> {
  await tx.execute(
    `INSERT INTO applied_moves (request_id, player, from_room, to_room, direction)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(request_id) DO NOTHING`,
    [requestId, move.player, move.from_room, move.to_room, move.direction],
  );
}
