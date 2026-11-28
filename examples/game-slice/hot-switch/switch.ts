import type { GameVersion } from '../src/game.ts';
import {
  CONTROL_TIMEOUT_MS,
  CROSS_PROBE_DELAY_MS,
  POLL_INTERVAL_MS,
  SERVER_EXIT_TIMEOUT_MS,
} from './config.ts';
import { armWindowWake, clearWindowWake, fireWindowWake } from './loops.ts';
import {
  drainCrossProbe,
  probeVersion,
  settleCrossProbe,
  startCrossSwitchProbe,
  waitForOrdinaryWindowSample,
} from './probes.ts';
import {
  asMs,
  check,
  fail,
  isProcessAlive,
  isRecord,
  messageOf,
  nowNs,
  readPid,
  sleep,
  withDeadline,
} from './support.ts';
import type {
  ControlReply,
  CrossSwitchProbe,
  DrillClient,
  Run,
} from './types.ts';

/** One real `switchTo` and the pid/window checks around it. */

export async function requestSwitch(
  run: Run,
  target: GameVersion,
): Promise<ControlReply> {
  const response = await withDeadline(
    fetch(run.portal.controlUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ version: target }),
    }),
    CONTROL_TIMEOUT_MS,
    `POST /switch {version:"${target}"}`,
  );
  const text = await response.text();
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new Error(
      `control endpoint returned non-JSON (HTTP ${response.status}): ${messageOf(error)}`,
      { cause: error },
    );
  }
  if (!isRecord(parsed)) {
    throw new Error(`control endpoint returned an unexpected body: ${text}`);
  }
  if (response.status !== 200 || parsed.ok !== true) {
    const reason =
      typeof parsed.error === 'string' ? parsed.error : 'no error message';
    throw new Error(
      `switch to ${target} failed (HTTP ${response.status}): ${reason}`,
    );
  }
  const durationMs = parsed.durationMs;
  return {
    durationMs: typeof durationMs === 'number' ? durationMs : 0,
    fromPid: readPid(parsed.fromPid),
    toPid: readPid(parsed.toPid),
  };
}

async function waitForPidExit(
  pid: number,
  timeoutMs: number,
): Promise<{ readonly exited: boolean; readonly waitMs: number }> {
  const startNs = nowNs();
  let exited = !isProcessAlive(pid);
  while (!exited && asMs(nowNs() - startNs) < timeoutMs) {
    // oxlint-disable-next-line eslint/no-await-in-loop -- bounded wait for one pid
    await sleep(POLL_INTERVAL_MS);
    exited = !isProcessAlive(pid);
  }
  return { exited, waitMs: asMs(nowNs() - startNs) };
}

export async function runSwitch(
  run: Run,
  anchor: DrillClient,
  watcher: DrillClient,
  from: GameVersion,
  target: GameVersion,
  expectedFromPid: number,
): Promise<number | undefined> {
  const label = `switch ${run.windows.length + 1} (${from} -> ${target})`;
  // Let the anchor finish an in-flight command first: only then can the wake
  // turn its next pause into a say that still lands inside the window.
  await anchor.queue.idle();
  const startNs = nowNs();
  run.activeSwitch = run.windows.length + 1;
  const wallStartNs = nowNs();
  armWindowWake(run);
  const switching = requestSwitch(run, target);
  let cross: CrossSwitchProbe | undefined;
  let reply: ControlReply;
  try {
    await sleep(CROSS_PROBE_DELAY_MS);
    cross = startCrossSwitchProbe(run, anchor, watcher);
    fireWindowWake(run);
    reply = await switching;
  } catch (error) {
    run.windows.push({
      index: run.windows.length,
      target,
      startNs,
      endNs: nowNs(),
    });
    run.activeSwitch = 0;
    await drainCrossProbe(cross);
    clearWindowWake(run);
    fail(run, `${label}: ${messageOf(error)}`);
    return undefined;
  }
  const wallMs = asMs(nowNs() - wallStartNs);
  const endNs = nowNs();
  run.windows.push({ index: run.windows.length, target, startNs, endNs });
  run.activeSwitch = 0;

  if (reply.fromPid !== undefined) {
    run.portal.notePid(reply.fromPid);
  }
  if (reply.toPid !== undefined) {
    run.portal.notePid(reply.toPid);
  }
  let oldExited = false;
  let oldExitWaitMs = 0;
  if (reply.fromPid === undefined || reply.toPid === undefined) {
    fail(run, `${label}: control endpoint did not report both pids`);
  } else {
    check(
      run,
      reply.fromPid === expectedFromPid,
      `${label}: replaced pid ${reply.fromPid}, expected ${expectedFromPid}`,
    );
    check(
      run,
      reply.fromPid !== reply.toPid,
      `${label}: fromPid and toPid are both ${reply.fromPid}`,
    );
    const wait = await waitForPidExit(reply.fromPid, SERVER_EXIT_TIMEOUT_MS);
    oldExited = wait.exited;
    oldExitWaitMs = wait.waitMs;
    check(
      run,
      oldExited,
      `${label}: previous Server pid ${reply.fromPid} is still alive ${SERVER_EXIT_TIMEOUT_MS}ms after the switch`,
    );
    check(
      run,
      isProcessAlive(reply.toPid),
      `${label}: replacement Server pid ${reply.toPid} is not alive`,
    );
  }
  const newAlive = reply.toPid !== undefined && isProcessAlive(reply.toPid);
  const crossResult = await settleCrossProbe(
    run,
    cross,
    label,
    startNs,
    endNs,
    target,
  );
  const ordinary = await waitForOrdinaryWindowSample(
    run,
    anchor,
    label,
    startNs,
    endNs,
    target,
  );
  clearWindowWake(run);
  const versionText = await probeVersion(run, anchor, watcher, target, label);
  run.switches.push({
    label,
    target,
    controlMs: reply.durationMs,
    wallMs,
    fromPid: reply.fromPid,
    toPid: reply.toPid,
    oldExited,
    oldExitWaitMs,
    newAlive,
    versionText,
    crossText: crossResult.text,
    crossEventMs: crossResult.latencyMs,
    ordinaryText: ordinary.text,
    ordinaryEventMs: ordinary.latencyMs,
  });
  return reply.toPid;
}
