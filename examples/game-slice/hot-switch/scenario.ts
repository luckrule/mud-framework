import { V2_SAY_PREFIX } from '../src/game.ts';
import { command, connectClients } from './clients.ts';
import {
  ANCHOR_ITEM,
  BETWEEN_SWITCHES_MS,
  EVENT_DRAIN_MS,
  REQUEST_TIMEOUT_MS,
  ROUTE_TO_TARGET,
  SETTLE_AFTER_LAST_SWITCH_MS,
  SWITCH_TARGETS,
  TARGET_ROOM,
  WARMUP_MS,
} from './config.ts';
import { startLoops } from './loops.ts';
import { probeVersion, verifyState } from './probes.ts';
import {
  fail,
  fromOf,
  isRecord,
  messageOf,
  nowNs,
  readInventory,
  readRoom,
  sleep,
  withDeadline,
} from './support.ts';
import { runSwitch } from './switch.ts';
import type { DrillClient, Run } from './types.ts';

/** Setup, the watcher's event accounting and the v1 -> v2 -> v1 sequence. */

async function walk(
  run: Run,
  state: DrillClient,
  directions: readonly string[],
  label: string,
): Promise<void> {
  for (const [step, direction] of directions.entries()) {
    // oxlint-disable-next-line eslint/no-await-in-loop -- a route is walked one exit at a time
    const moved = await command(
      run,
      state,
      'game.move',
      { direction },
      `${state.name}:setup:move:${step}`,
    );
    if (!moved.ok) {
      throw new Error(
        `${label}: move ${direction} failed (${moved.code}: ${moved.message})`,
      );
    }
    state.room = readRoom(moved.result).id;
  }
}

async function stationClients(
  run: Run,
  anchor: DrillClient,
  watcher: DrillClient,
): Promise<readonly string[]> {
  const taken = await command(
    run,
    anchor,
    'game.take',
    { item: ANCHOR_ITEM },
    `${anchor.name}:setup:take`,
  );
  if (!taken.ok) {
    throw new Error(
      `take ${ANCHOR_ITEM} failed (${taken.code}: ${taken.message})`,
    );
  }
  const inventory = await command(
    run,
    anchor,
    'game.inventory',
    undefined,
    `${anchor.name}:setup:inventory`,
  );
  if (!inventory.ok) {
    throw new Error(
      `inventory failed (${inventory.code}: ${inventory.message})`,
    );
  }
  const itemIds = readInventory(inventory.result).itemIds;
  if (!itemIds.includes(ANCHOR_ITEM)) {
    throw new Error(
      `inventory does not contain "${ANCHOR_ITEM}" after take: [${itemIds.join(', ')}]`,
    );
  }
  if (anchor.room !== 'square') {
    throw new Error(`anchor starts in "${anchor.room}", expected "square"`);
  }
  await walk(run, anchor, ROUTE_TO_TARGET, 'anchor');
  await walk(run, watcher, ROUTE_TO_TARGET, 'watcher');
  for (const state of [anchor, watcher]) {
    // oxlint-disable-next-line eslint/no-await-in-loop -- two clients, one check each
    const looked = await command(
      run,
      state,
      'game.look',
      undefined,
      `${state.name}:setup:look`,
    );
    if (!looked.ok) {
      throw new Error(
        `${state.name}: setup look failed (${looked.code}: ${looked.message})`,
      );
    }
    const room = readRoom(looked.result);
    if (room.id !== TARGET_ROOM) {
      throw new Error(
        `${state.name}: setup ended in "${room.id}", expected "${TARGET_ROOM}"`,
      );
    }
    state.room = room.id;
  }
  return itemIds;
}

/** Records the anchor's say events with their send-to-event latency. */
function watchSays(run: Run, anchor: DrillClient, watcher: DrillClient): void {
  watcher.client.onEvent((event) => {
    if (event.event !== 'room.say' || fromOf(event) !== anchor.name) {
      return;
    }
    const text = isRecord(event.payload) ? event.payload.text : undefined;
    if (typeof text !== 'string') {
      return;
    }
    const marker = text.startsWith(V2_SAY_PREFIX)
      ? text.slice(V2_SAY_PREFIX.length)
      : text;
    const sentAtNs = run.anchorSendTimes.get(marker);
    if (sentAtNs === undefined || run.anchorObserved.has(marker)) {
      return;
    }
    run.anchorObserved.add(marker);
    const atNs = nowNs();
    run.eventSamples.push({
      sentAtNs,
      atNs,
      latencyNs: Math.max(0, atNs - sentAtNs),
      marker,
      text,
    });
  });
}

export async function scenario(run: Run): Promise<void> {
  const clients = await connectClients(run.portal.url, run.runTag);
  run.clients.push(...clients);
  const anchor = clients[0];
  const watcher = clients[1];
  if (anchor === undefined || watcher === undefined) {
    throw new Error('expected at least two clients');
  }
  const expectedItemIds = await stationClients(run, anchor, watcher);
  watchSays(run, anchor, watcher);

  // The initial Server must be v1: no prefix before the first switch.
  await probeVersion(run, anchor, watcher, 'v1', 'initial server');
  await verifyState(run, anchor, 'before-switches', expectedItemIds);

  const loops = startLoops(run, clients);
  await sleep(WARMUP_MS);

  let expectedFromPid = run.portal.initialPid;
  for (const [index, target] of SWITCH_TARGETS.entries()) {
    if (index > 0) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- the switches are a sequence
      await sleep(BETWEEN_SWITCHES_MS);
    }
    const from = index === 0 ? 'v1' : SWITCH_TARGETS[index - 1];
    if (from === undefined) {
      throw new Error('switch sequence lost its previous version');
    }
    // oxlint-disable-next-line eslint/no-await-in-loop -- the switches are a sequence
    const toPid = await runSwitch(
      run,
      anchor,
      watcher,
      from,
      target,
      expectedFromPid,
    );
    if (toPid !== undefined) {
      expectedFromPid = toPid;
    }
    // oxlint-disable-next-line eslint/no-await-in-loop -- verification follows the switch it inspects
    await verifyState(run, anchor, `after-${target}`, expectedItemIds);
  }

  await sleep(SETTLE_AFTER_LAST_SWITCH_MS);
  run.stopping = true;
  try {
    const results = await withDeadline(
      Promise.allSettled(loops),
      REQUEST_TIMEOUT_MS + 1000,
      'client loops',
    );
    for (const result of results) {
      if (result.status === 'rejected') {
        fail(run, `client loop failed: ${messageOf(result.reason)}`);
      }
    }
  } catch (error) {
    fail(run, `client loops did not stop: ${messageOf(error)}`);
  }
  // Events already in flight still count for the gap report.
  await sleep(EVENT_DRAIN_MS);
}
