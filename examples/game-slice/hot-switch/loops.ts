import { command, sayCommand } from './clients.ts';
import {
  FLEET_DOORS,
  FLEET_TARGETS,
  INTERVAL_MS,
  STAGGER_MS,
} from './config.ts';
import { fail, nowNs, readRoom, sleep } from './support.ts';
import type { DrillClient, Run } from './types.ts';

/** The offered load: the wake-up hook, the anchor/watcher loop and the fleet. */

/** Slice size for the anchor's interruptible sleep. */
const WAKE_POLL_MS = 10;

export function armWindowWake(run: Run): void {
  run.windowWake = { fired: false };
}

export function fireWindowWake(run: Run): void {
  const wake = run.windowWake;
  if (wake !== undefined) {
    wake.fired = true;
  }
}

export function clearWindowWake(run: Run): void {
  run.windowWake = undefined;
}

/**
 * The anchor's pause is checked in short slices while a switch is being
 * triggered, so a sleep that began before the switch is still interrupted in
 * time to submit a regular `say` inside the window. Other clients sleep whole.
 */
async function pause(run: Run, markerSay: boolean): Promise<void> {
  if (!markerSay) {
    await sleep(INTERVAL_MS);
    return;
  }
  const deadline = nowNs() + INTERVAL_MS * 1e6;
  for (;;) {
    if (run.windowWake?.fired === true) {
      return;
    }
    const remaining = deadline - nowNs();
    if (remaining <= 0) {
      return;
    }
    // oxlint-disable-next-line eslint/no-await-in-loop -- the slice is the wake's latency bound
    await sleep(Math.min(remaining / 1e6, WAKE_POLL_MS));
  }
}

type Cycle = (
  run: Run,
  state: DrillClient,
  seq: number,
  markerSay: boolean,
) => Promise<boolean>;

async function runLoop(
  run: Run,
  state: DrillClient,
  markerSay: boolean,
  cycle: Cycle,
): Promise<void> {
  let seq = 0;
  while (!run.stopping) {
    seq += 1;
    // oxlint-disable-next-line eslint/no-await-in-loop -- one cycle continues the previous one
    const keepGoing = await cycle(run, state, seq, markerSay);
    if (!keepGoing) {
      return;
    }
  }
}

async function lookSayCycle(
  run: Run,
  state: DrillClient,
  seq: number,
  markerSay: boolean,
): Promise<boolean> {
  // A fired wake means the next command must be the say: going through the
  // look first would let the switch buffer that look and delay the say past
  // the window.
  const wakeNow = markerSay && run.windowWake?.fired === true;
  if (wakeNow) {
    clearWindowWake(run);
  } else {
    const looked = await command(
      run,
      state,
      'game.look',
      undefined,
      `${state.name}:look:${seq}`,
    );
    if (run.stopping) {
      return false;
    }
    if (!looked.ok) {
      fail(
        run,
        `${state.name}: look failed (${looked.code}: ${looked.message})`,
      );
      return false;
    }
    state.room = readRoom(looked.result).id;
    await pause(run, markerSay);
    if (run.stopping) {
      return false;
    }
    if (markerSay && run.windowWake?.fired === true) {
      clearWindowWake(run);
    }
  }
  const sayId = `${state.name}:say:${seq}`;
  const said = markerSay
    ? await sayCommand(
        run,
        state,
        `hs-say-${state.name}-${seq}`,
        sayId,
        `hs-say-${state.name}-${seq}`,
      )
    : await sayCommand(
        run,
        state,
        `${state.name} tick ${seq}`,
        sayId,
        undefined,
      );
  if (!said) {
    return false;
  }
  await pause(run, markerSay);
  return !run.stopping;
}

/**
 * The retry of a committed move is refused with `NO_EXIT` (the player is
 * already in the target room). Reading the room tells "the original commit
 * happened" apart from a real failure without a new storage table.
 */
async function recoverMove(
  run: Run,
  state: DrillClient,
  direction: string,
  seq: number,
): Promise<boolean> {
  const looked = await command(
    run,
    state,
    'game.look',
    undefined,
    `${state.name}:resync:${seq}`,
  );
  if (!looked.ok) {
    fail(
      run,
      `${state.name}: could not resync after a retried move (${looked.code}: ${looked.message})`,
    );
    return false;
  }
  const room = readRoom(looked.result).id;
  state.room = room;
  const target = FLEET_TARGETS[direction];
  if (room === target) {
    state.stats.recoveredMoves += 1;
    return true;
  }
  fail(
    run,
    `${state.name}: move ${direction} ended with NO_EXIT but the room is ${room}, not ${target}`,
  );
  return false;
}

async function fleetCycle(
  run: Run,
  state: DrillClient,
  seq: number,
): Promise<boolean> {
  const looked = await command(
    run,
    state,
    'game.look',
    undefined,
    `${state.name}:look:${seq}`,
  );
  if (run.stopping) {
    return false;
  }
  if (!looked.ok) {
    fail(run, `${state.name}: look failed (${looked.code}: ${looked.message})`);
    return false;
  }
  state.room = readRoom(looked.result).id;
  await sleep(INTERVAL_MS);
  if (run.stopping) {
    return false;
  }
  const said = await sayCommand(
    run,
    state,
    `${state.name} tick ${seq}`,
    `${state.name}:say:${seq}`,
    undefined,
  );
  if (!said) {
    return false;
  }
  await sleep(INTERVAL_MS);
  if (run.stopping) {
    return false;
  }
  const direction = FLEET_DOORS[state.room];
  if (direction !== undefined) {
    const moved = await command(
      run,
      state,
      'game.move',
      { direction },
      `${state.name}:move:${seq}`,
    );
    if (run.stopping) {
      return false;
    }
    if (moved.ok) {
      state.room = readRoom(moved.result).id;
    } else if (moved.code === 'NO_EXIT' && moved.attempts > 1) {
      if (!(await recoverMove(run, state, direction, seq))) {
        return false;
      }
    } else {
      fail(
        run,
        `${state.name}: move ${direction} failed (${moved.code}: ${moved.message})`,
      );
      return false;
    }
  }
  await sleep(INTERVAL_MS);
  return !run.stopping;
}

async function staggeredLoop(
  run: Run,
  offsetMs: number,
  loop: () => Promise<void>,
): Promise<void> {
  if (offsetMs > 0) {
    await sleep(offsetMs);
  }
  if (!run.stopping) {
    await loop();
  }
}

export function startLoops(
  run: Run,
  clients: readonly DrillClient[],
): Promise<void>[] {
  const anchor = clients[0];
  const watcher = clients[1];
  if (anchor === undefined || watcher === undefined) {
    throw new Error('expected at least two clients');
  }
  const loops = [
    staggeredLoop(run, 0, () => runLoop(run, anchor, true, lookSayCycle)),
    staggeredLoop(run, STAGGER_MS, () =>
      runLoop(run, watcher, false, lookSayCycle),
    ),
  ];
  for (const [offset, state] of clients.slice(2).entries()) {
    loops.push(
      staggeredLoop(run, (offset + 2) * STAGGER_MS, () =>
        runLoop(run, state, false, fleetCycle),
      ),
    );
  }
  return loops;
}
