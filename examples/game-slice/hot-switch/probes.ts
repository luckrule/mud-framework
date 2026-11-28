import type { GameEvent } from '../src/client-lib.ts';
import type { GameVersion } from '../src/game.ts';
import { command, sayCommand } from './clients.ts';
import { POLL_INTERVAL_MS, PROBE_TIMEOUT_MS, TARGET_ROOM } from './config.ts';
import {
  asMs,
  check,
  expectedSayText,
  fail,
  fromOf,
  messageOf,
  nowNs,
  readInventory,
  readRoom,
  sayTextOf,
  sleep,
} from './support.ts';
import type { CrossSwitchProbe, DrillClient, Run } from './types.ts';

/** The in-window say probes, the version probe and the post-switch checks. */

export interface ProbeResult {
  readonly text: string;
  readonly latencyMs: number | undefined;
}

export function startCrossSwitchProbe(
  run: Run,
  anchor: DrillClient,
  watcher: DrillClient,
): CrossSwitchProbe {
  const marker = `hs-cross-${run.probes}`;
  run.probes += 1;
  return {
    marker,
    observed: watcher.client.nextEvent(
      (event) =>
        event.event === 'room.say' &&
        fromOf(event) === anchor.name &&
        sayTextOf(event).includes(marker),
      PROBE_TIMEOUT_MS,
    ),
    said: sayCommand(
      run,
      anchor,
      marker,
      `${anchor.name}:cross:${marker}`,
      marker,
    ),
  };
}

/** Settles a probe whose switch failed, so no rejection goes unhandled. */
export async function drainCrossProbe(
  cross: CrossSwitchProbe | undefined,
): Promise<void> {
  if (cross === undefined) {
    return;
  }
  const said = await cross.said;
  if (said) {
    await cross.observed.catch(() => undefined);
    return;
  }
  void cross.observed.catch(() => undefined);
}

/** The injected sample must be submitted inside the window and name its version. */
export async function settleCrossProbe(
  run: Run,
  cross: CrossSwitchProbe | undefined,
  label: string,
  windowStartNs: number,
  windowEndNs: number,
  target: GameVersion,
): Promise<ProbeResult> {
  if (cross === undefined) {
    fail(run, `${label}: the cross-switch say was never issued`);
    return { text: '', latencyMs: undefined };
  }
  const said = await cross.said;
  if (!said) {
    void cross.observed.catch(() => undefined);
    return { text: '', latencyMs: undefined };
  }
  let event: GameEvent;
  try {
    event = await cross.observed;
  } catch (error) {
    fail(
      run,
      `${label}: no room.say for the cross-switch probe within ${PROBE_TIMEOUT_MS}ms (${messageOf(error)})`,
    );
    return { text: '', latencyMs: undefined };
  }
  const text = sayTextOf(event);
  const sample = run.eventSamples.find(
    (candidate) => candidate.marker === cross.marker,
  );
  check(
    run,
    sample !== undefined &&
      sample.sentAtNs >= windowStartNs &&
      sample.sentAtNs <= windowEndNs,
    `${label}: the cross-switch say was not submitted inside the switch window`,
  );
  check(
    run,
    text === expectedSayText(target, cross.marker),
    `${label}: cross-switch say text ${JSON.stringify(text)} does not match ${target}`,
  );
  return {
    text,
    latencyMs: sample === undefined ? undefined : asMs(sample.latencyNs),
  };
}

/**
 * At least one ordinary load say (the anchor's regular loop, woken for the
 * window) must also be submitted inside it: the injected probe alone is not
 * the coverage claim.
 */
export async function waitForOrdinaryWindowSample(
  run: Run,
  anchor: DrillClient,
  label: string,
  windowStartNs: number,
  windowEndNs: number,
  target: GameVersion,
): Promise<ProbeResult> {
  const prefix = `hs-say-${anchor.name}-`;
  const deadline = nowNs() + PROBE_TIMEOUT_MS * 1e6;
  for (;;) {
    const sample = run.eventSamples.find(
      (candidate) =>
        candidate.marker.startsWith(prefix) &&
        candidate.sentAtNs >= windowStartNs &&
        candidate.sentAtNs <= windowEndNs,
    );
    if (sample !== undefined) {
      check(
        run,
        sample.text === expectedSayText(target, sample.marker),
        `${label}: ordinary anchor say text ${JSON.stringify(sample.text)} does not match ${target}`,
      );
      return { text: sample.text, latencyMs: asMs(sample.latencyNs) };
    }
    if (nowNs() >= deadline) {
      fail(
        run,
        `${label}: no ordinary anchor say was submitted inside the switch window`,
      );
      return { text: '', latencyMs: undefined };
    }
    // oxlint-disable-next-line eslint/no-await-in-loop -- bounded wait for the event
    await sleep(POLL_INTERVAL_MS);
  }
}

export async function probeVersion(
  run: Run,
  anchor: DrillClient,
  watcher: DrillClient,
  expected: GameVersion,
  label: string,
): Promise<string> {
  const marker = `hs-version-${run.probes}`;
  run.probes += 1;
  const observed = watcher.client.nextEvent(
    (event) =>
      event.event === 'room.say' &&
      fromOf(event) === anchor.name &&
      sayTextOf(event).includes(marker),
    PROBE_TIMEOUT_MS,
  );
  const said = await sayCommand(
    run,
    anchor,
    marker,
    `${anchor.name}:probe:${marker}`,
    marker,
  );
  if (!said) {
    void observed.catch(() => undefined);
    return '';
  }
  let event: GameEvent;
  try {
    event = await observed;
  } catch (error) {
    fail(
      run,
      `${label}: no room.say for the version probe within ${PROBE_TIMEOUT_MS}ms (${messageOf(error)})`,
    );
    return '';
  }
  const text = sayTextOf(event);
  const expectedText = expectedSayText(expected, marker);
  check(
    run,
    text === expectedText,
    `${label}: expected room.say ${JSON.stringify(expectedText)}, got ${JSON.stringify(text)}`,
  );
  return text;
}

export async function verifyState(
  run: Run,
  anchor: DrillClient,
  label: string,
  expectedItemIds: readonly string[],
): Promise<void> {
  const inventory = await command(
    run,
    anchor,
    'game.inventory',
    undefined,
    `${anchor.name}:inventory:${label}`,
  );
  if (!inventory.ok) {
    fail(
      run,
      `${label}: inventory failed (${inventory.code}: ${inventory.message})`,
    );
    return;
  }
  const { itemIds } = readInventory(inventory.result);
  for (const id of expectedItemIds) {
    check(
      run,
      itemIds.includes(id),
      `${label}: inventory lost "${id}" (has [${itemIds.join(', ')}])`,
    );
  }
  const looked = await command(
    run,
    anchor,
    'game.look',
    undefined,
    `${anchor.name}:verify-look:${label}`,
  );
  if (!looked.ok) {
    fail(run, `${label}: look failed (${looked.code}: ${looked.message})`);
    return;
  }
  const room = readRoom(looked.result);
  check(
    run,
    room.id === TARGET_ROOM,
    `${label}: anchor is in "${room.id}", expected "${TARGET_ROOM}"`,
  );
  check(
    run,
    room.you === anchor.name,
    `${label}: look ran as "${room.you}", expected "${anchor.name}"`,
  );
  // A session-scoped handler succeeding (and naming this player) proves the
  // Connection still holds the Session bound at login: a lost binding would
  // fail with AUTH_REQUIRED.
  check(
    run,
    anchor.sessionId === `player:${anchor.name}`,
    `${label}: session id is "${anchor.sessionId}", expected "player:${anchor.name}"`,
  );
  run.stateChecks.push({
    label,
    itemIds,
    room: room.id,
    you: room.you,
    sessionId: anchor.sessionId,
  });
}
