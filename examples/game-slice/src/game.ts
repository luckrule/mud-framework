import { BusinessError } from 'mud-framework-core';
import type { Extension, RequestContext } from 'mud-framework-core';

import type { AppliedMove, ItemRow, ItemView, PlayerRow } from './world.ts';
import {
  MAX_ITEM_TERM_LENGTH,
  MAX_NAME_LENGTH,
  MAX_SAY_LENGTH,
  START_ROOM,
  clearPlayerSessionIfHolder,
  dropItem,
  findAppliedMove,
  findExit,
  findItemByTerm,
  findItemOnFloor,
  findItemOwnedBy,
  findPlayerBySession,
  listInventory,
  listOtherSessions,
  recordMove,
  roomSnapshot,
  setPlayerRoom,
  takeItem,
  upsertPlayerLogin,
} from './world.ts';

/** Wire payloads of the Server events this Extension emits (ADR-0008). */
export interface RoomMoveEvent {
  readonly player: string;
  readonly direction: string;
  readonly from: string;
  readonly to: string;
}

export interface RoomSayEvent {
  readonly from: string;
  readonly text: string;
}

export interface ItemEvent {
  readonly player: string;
  readonly item: ItemView;
  readonly room: string;
}

/**
 * The `game` Extension. No authentication is involved: `game.login` binds the
 * Session named after whatever non-empty name the client sends. That is a
 * demo-only identity rule, not an authentication scheme.
 */
export function gameExtension(): Extension {
  return {
    name: 'game',
    requires: { transactions: 'full', uniqueConstraint: true },
    register: (registrar) => {
      registrar.register(
        'game.login',
        async (ctx, payload) => {
          const name = readName(payload);
          // Stable per name: the same player logging in on a new connection
          // rebinds this Session, and the Portal takes the route over from the
          // previous connection (SPEC 3.2). Replay is an upsert.
          const sessionId = `player:${name}`;
          const player = await upsertPlayerLogin(
            ctx.storage,
            name,
            sessionId,
            START_ROOM,
            ctx.connectionId,
          );
          ctx.bindSession(sessionId);
          return {
            sessionId,
            room: await roomSnapshot(ctx.storage, player.room_id, name),
          };
        },
        { access: 'public', mayChangeSession: true },
      );

      registrar.register(
        'game.logout',
        async (ctx) => {
          const player = await currentPlayer(ctx);
          ctx.unbindSession();
          // A same-name login on another connection may have committed after
          // this request entered the framework. Only the current holder may
          // clear the online marker; the stale connection still unbinds its
          // own route, but must not strand the new holder.
          const cleared = await clearPlayerSessionIfHolder(
            ctx.storage,
            player.name,
            ctx.connectionId,
          );
          return cleared
            ? { loggedOut: true }
            : { loggedOut: false, superseded: true };
        },
        { access: 'session', mayChangeSession: true },
      );

      registrar.register('game.look', async (ctx) => {
        const player = await currentPlayer(ctx);
        return {
          room: await roomSnapshot(ctx.storage, player.room_id, player.name),
        };
      });

      registrar.register('game.move', async (ctx, payload) => {
        const direction = readDirection(payload);
        const player = await currentPlayer(ctx);

        // A replayed request carries the same `requestId` as the committed
        // original. Re-emit its events (best-effort; the replay repeats the
        // same `eventId`) but do not move the player again.
        const applied = await findAppliedMove(ctx.storage, ctx.requestId);
        if (applied !== undefined) {
          await emitMoveEvents(ctx, applied);
          return {
            moved: true,
            replayed: true,
            room: await roomSnapshot(ctx.storage, player.room_id, player.name),
          };
        }

        const from = player.room_id;
        const to = await findExit(ctx.storage, from, direction);
        if (to === undefined) {
          throw new BusinessError(
            'NO_EXIT',
            `there is no exit "${direction}" from "${from}"`,
          );
        }
        const leaving = await listOtherSessions(ctx.storage, from, player.name);
        const entering = await listOtherSessions(ctx.storage, to, player.name);
        const move: AppliedMove = {
          player: player.name,
          from_room: from,
          to_room: to,
          direction,
        };
        await setPlayerRoom(ctx.storage, player.name, to);
        await recordMove(ctx.storage, ctx.requestId, move);
        await emitMoveEvents(ctx, move, leaving, entering);
        return {
          moved: true,
          room: await roomSnapshot(ctx.storage, to, player.name),
        };
      });

      registrar.register('game.say', async (ctx, payload) => {
        const text = readSayText(payload);
        const player = await currentPlayer(ctx);
        const listeners = await listOtherSessions(
          ctx.storage,
          player.room_id,
          player.name,
        );
        const eventPayload: RoomSayEvent = { from: player.name, text };
        for (const sessionId of listeners) {
          ctx.emitToSession(sessionId, 'room.say', eventPayload);
        }
        return { said: true };
      });

      registrar.register('game.take', async (ctx, payload) => {
        const term = readItemTerm(payload);
        const player = await currentPlayer(ctx);

        const owned = await findItemOwnedBy(ctx.storage, player.name, term);
        if (owned !== undefined) {
          // Repeated take (a client retry or a replay) stays successful and
          // does not move the item again.
          return { item: viewOf(owned), alreadyOwned: true };
        }
        const item = await findItemOnFloor(ctx.storage, player.room_id, term);
        if (item === undefined) {
          throw await itemUnavailable(ctx, term, player.room_id);
        }
        const taken = await takeItem(
          ctx.storage,
          item.id,
          player.name,
          player.room_id,
        );
        if (!taken) {
          throw new BusinessError(
            'ITEM_TAKEN',
            `"${item.name}" was taken by someone else`,
          );
        }
        await emitItemEvent(ctx, player, item, 'item.taken');
        return { item: viewOf(item), alreadyOwned: false };
      });

      registrar.register('game.drop', async (ctx, payload) => {
        const term = readItemTerm(payload);
        const player = await currentPlayer(ctx);

        const owned = await findItemOwnedBy(ctx.storage, player.name, term);
        if (owned === undefined) {
          const here = await findItemOnFloor(ctx.storage, player.room_id, term);
          if (here !== undefined) {
            // Already on this floor: a repeated drop is a successful no-op.
            return { item: viewOf(here), alreadyHere: true };
          }
          throw new BusinessError(
            'NOT_OWNED',
            `you are not carrying anything called "${term}"`,
          );
        }
        const dropped = await dropItem(
          ctx.storage,
          owned.id,
          player.name,
          player.room_id,
        );
        if (!dropped) {
          throw new BusinessError(
            'NOT_OWNED',
            `"${owned.name}" could not be dropped`,
          );
        }
        await emitItemEvent(ctx, player, owned, 'item.dropped');
        return { item: viewOf(owned), alreadyHere: false };
      });

      registrar.register('game.inventory', async (ctx) => {
        const player = await currentPlayer(ctx);
        return { items: await listInventory(ctx.storage, player.name) };
      });
    },
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readString(payload: unknown, field: string): string {
  const value = isRecord(payload) ? payload[field] : undefined;
  if (typeof value !== 'string') {
    throw new BusinessError(
      'BAD_REQUEST',
      `expected payload.${field} to be a string`,
    );
  }
  return value;
}

function readName(payload: unknown): string {
  const name = readString(payload, 'name').trim();
  if (name.length === 0) {
    throw new BusinessError('BAD_REQUEST', 'name must not be empty');
  }
  if (name.length > MAX_NAME_LENGTH) {
    throw new BusinessError(
      'BAD_REQUEST',
      `name must be at most ${MAX_NAME_LENGTH} characters`,
    );
  }
  return name;
}

function readDirection(payload: unknown): string {
  const direction = readString(payload, 'direction').trim().toLowerCase();
  if (direction.length === 0) {
    throw new BusinessError('BAD_REQUEST', 'direction must not be empty');
  }
  return direction;
}

function readSayText(payload: unknown): string {
  const text = readString(payload, 'text').trim();
  if (text.length === 0) {
    throw new BusinessError('BAD_REQUEST', 'text must not be empty');
  }
  if (text.length > MAX_SAY_LENGTH) {
    throw new BusinessError(
      'BAD_REQUEST',
      `text must be at most ${MAX_SAY_LENGTH} characters`,
    );
  }
  return text;
}

function readItemTerm(payload: unknown): string {
  const term = readString(payload, 'item').trim();
  if (term.length === 0) {
    throw new BusinessError('BAD_REQUEST', 'item must not be empty');
  }
  if (term.length > MAX_ITEM_TERM_LENGTH) {
    throw new BusinessError(
      'BAD_REQUEST',
      `item must be at most ${MAX_ITEM_TERM_LENGTH} characters`,
    );
  }
  return term;
}

async function currentPlayer(ctx: RequestContext): Promise<PlayerRow> {
  const sessionId = ctx.sessionId;
  if (sessionId === undefined || sessionId.length === 0) {
    // `access: 'session'` already refused this request; the guard is a
    // framework-invariant check, not a business refusal.
    throw new Error('game handler ran without a session');
  }
  const player = await findPlayerBySession(ctx.storage, sessionId);
  if (player === undefined) {
    throw new BusinessError(
      'UNKNOWN_PLAYER',
      `no player is bound to session "${sessionId}"`,
    );
  }
  return player;
}

async function emitMoveEvents(
  ctx: RequestContext,
  move: AppliedMove,
  leaving?: readonly string[],
  entering?: readonly string[],
): Promise<void> {
  const payload: RoomMoveEvent = {
    player: move.player,
    direction: move.direction,
    from: move.from_room,
    to: move.to_room,
  };
  const fromSessions =
    leaving ??
    (await listOtherSessions(ctx.storage, move.from_room, move.player));
  const toSessions =
    entering ??
    (await listOtherSessions(ctx.storage, move.to_room, move.player));
  for (const sessionId of fromSessions) {
    ctx.emitToSession(sessionId, 'room.left', payload);
  }
  for (const sessionId of toSessions) {
    ctx.emitToSession(sessionId, 'room.entered', payload);
  }
}

async function emitItemEvent(
  ctx: RequestContext,
  player: PlayerRow,
  item: ItemRow | ItemView,
  event: 'item.taken' | 'item.dropped',
): Promise<void> {
  const listeners = await listOtherSessions(
    ctx.storage,
    player.room_id,
    player.name,
  );
  const payload: ItemEvent = {
    player: player.name,
    item: viewOf(item),
    room: player.room_id,
  };
  for (const sessionId of listeners) {
    ctx.emitToSession(sessionId, event, payload);
  }
}

function viewOf(item: ItemRow | ItemView): ItemView {
  return { id: item.id, name: item.name };
}

async function itemUnavailable(
  ctx: RequestContext,
  term: string,
  roomId: string,
): Promise<BusinessError> {
  const item = await findItemByTerm(ctx.storage, term);
  if (item === undefined) {
    return new BusinessError('NO_SUCH_ITEM', `no item called "${term}"`);
  }
  if (item.owner !== null) {
    return new BusinessError('ITEM_TAKEN', `"${item.name}" is already held`);
  }
  return new BusinessError(
    'ITEM_NOT_HERE',
    `"${item.name}" is not in "${roomId}"`,
  );
}
