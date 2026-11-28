import {
  MAX_REPEATS_PER_WINDOW,
  MIN_FLEET_LOOKS,
  MIN_FLEET_MOVES,
  MIN_FLEET_SAYS,
  MIN_STATIONARY_LOOKS,
  MIN_STATIONARY_SAYS,
  P95_LIMIT_MS,
  SCRIPT_BUDGET_MS,
} from './config.ts';
import {
  eventLatenciesInWindow,
  latencyText,
  requestLatenciesInWindow,
  summarizeLatency,
} from './metrics.ts';
import type { Aggregate, SayAccounting } from './metrics.ts';
import { check, fail, formatMs, write } from './support.ts';
import type { Run, SwitchReport, SwitchWindow } from './types.ts';

/** The human-readable report and the assertions that decide PASS/FAIL. */

function reportSwitch(switchReport: SwitchReport): void {
  const old = switchReport.oldExited
    ? `exited after ${formatMs(switchReport.oldExitWaitMs)} ms`
    : 'still alive';
  write(
    `${switchReport.label}: control ${formatMs(switchReport.controlMs)} ms, wall ${formatMs(switchReport.wallMs)} ms, pid ${String(switchReport.fromPid)} -> ${String(switchReport.toPid)} (old ${old}, new alive ${switchReport.newAlive ? 'yes' : 'no'})`,
  );
  write(`  version say: ${JSON.stringify(switchReport.versionText)}`);
  write(
    `  cross-switch say: ${JSON.stringify(switchReport.crossText)} (${switchReport.crossEventMs === undefined ? 'no event' : `${formatMs(switchReport.crossEventMs)} ms send-to-event`})`,
  );
  write(
    `  ordinary window say: ${JSON.stringify(switchReport.ordinaryText)} (${switchReport.ordinaryEventMs === undefined ? 'no event' : `${formatMs(switchReport.ordinaryEventMs)} ms send-to-event`})`,
  );
}

function reportWindow(run: Run, window: SwitchWindow): void {
  write(
    `  ${window.target} window requests: ${latencyText(summarizeLatency(requestLatenciesInWindow(run, window)))}`,
  );
  write(
    `  ${window.target} window events: ${latencyText(summarizeLatency(eventLatenciesInWindow(run, window)))}`,
  );
}

export function reportMetrics(
  run: Run,
  aggregateStats: Aggregate,
  says: SayAccounting,
  elapsedMs: number,
): void {
  write('');
  write(
    `hot-switch drill: ${run.clients.length} clients, ${run.windows.length} switches`,
  );
  write(
    `  portal: ${run.portal.url}  control ${run.portal.controlUrl}  initial pid ${run.portal.initialPid}  data ${run.dataDirectory}`,
  );
  for (const switchReport of run.switches) {
    reportSwitch(switchReport);
  }
  for (const stateCheck of run.stateChecks) {
    write(
      `  ${stateCheck.label}: inventory [${stateCheck.itemIds.join(', ')}], room ${stateCheck.room}, you ${stateCheck.you}, session ${stateCheck.sessionId}`,
    );
  }
  write(
    `requests: attempts ${aggregateStats.attempts}, ok ${aggregateStats.ok}, client-timeout ${aggregateStats.clientTimeouts}, framework-timeout ${aggregateStats.frameworkTimeouts}, failed ${aggregateStats.failures}, disconnected ${aggregateStats.disconnects}`,
  );
  write(
    `  latency ms (all attempts, failures included): ${latencyText(summarizeLatency(aggregateStats.latenciesNs))}`,
  );
  if (aggregateStats.codes.size > 0) {
    const codes = [...aggregateStats.codes]
      .map(([code, count]) => `${code}=${count}`)
      .join(', ');
    write(`  error codes: ${codes}`);
  }
  write(
    `  commands: looks ${aggregateStats.looks}, says ${aggregateStats.says}, moves ${aggregateStats.moves}`,
  );
  write(
    `  retries: ${aggregateStats.retried} (${aggregateStats.retrySucceeded} succeeded on retry, ${aggregateStats.recoveredMoves} moves recovered by look)`,
  );
  write(
    `  repeats: ${aggregateStats.repeats} total, ${aggregateStats.repeatsInSwitches} inside a switch, max per client per window ${aggregateStats.maxRepeatsPerWindow}; these are client retries by clientRequestId, not proof of server-side duplicate execution`,
  );
  for (const window of run.windows) {
    reportWindow(run, window);
  }
  write(
    `events: ${latencyText(summarizeLatency(run.eventSamples.map((sample) => sample.latencyNs)))}; say gaps (anchor -> watcher): ${says.missing.length} missing, ${says.unconfirmed.length} unconfirmed (report only)`,
  );
  write(`elapsed: ${formatMs(elapsedMs)} ms (budget ${SCRIPT_BUDGET_MS} ms)`);
}

export function judge(
  run: Run,
  aggregateStats: Aggregate,
  elapsedMs: number,
): void {
  const requestLatency = summarizeLatency(aggregateStats.latenciesNs);
  const eventLatency = summarizeLatency(
    run.eventSamples.map((sample) => sample.latencyNs),
  );
  check(
    run,
    aggregateStats.disconnects === 0,
    `${aggregateStats.disconnects} client request(s) lost the connection`,
  );
  check(
    run,
    aggregateStats.maxRepeatsPerWindow <= MAX_REPEATS_PER_WINDOW,
    `a client repeated ${aggregateStats.maxRepeatsPerWindow} command(s) within one window (limit ${MAX_REPEATS_PER_WINDOW})`,
  );
  check(
    run,
    requestLatency !== undefined && requestLatency.p95Ms < P95_LIMIT_MS,
    `request p95 (all attempts) ${formatMs(requestLatency?.p95Ms ?? Number.POSITIVE_INFINITY)} ms is not below ${P95_LIMIT_MS} ms`,
  );
  check(
    run,
    eventLatency !== undefined && eventLatency.p95Ms < P95_LIMIT_MS,
    `event p95 ${formatMs(eventLatency?.p95Ms ?? Number.POSITIVE_INFINITY)} ms is not below ${P95_LIMIT_MS} ms`,
  );
  for (const window of run.windows) {
    const windowRequests = summarizeLatency(
      requestLatenciesInWindow(run, window),
    );
    if (windowRequests === undefined) {
      fail(
        run,
        `${window.target} switch window has no request samples; the load was not sustained through the switch`,
      );
    } else {
      check(
        run,
        windowRequests.p95Ms < P95_LIMIT_MS,
        `${window.target} switch window request p95 ${formatMs(windowRequests.p95Ms)} ms is not below ${P95_LIMIT_MS} ms`,
      );
    }
    const windowEvents = summarizeLatency(eventLatenciesInWindow(run, window));
    if (windowEvents === undefined) {
      fail(
        run,
        `${window.target} switch window has no event samples; the injected and ordinary window says are missing`,
      );
    } else {
      check(
        run,
        windowEvents.p95Ms < P95_LIMIT_MS,
        `${window.target} switch window event p95 ${formatMs(windowEvents.p95Ms)} ms is not below ${P95_LIMIT_MS} ms`,
      );
    }
  }
  for (const state of run.clients) {
    const stationary = state.index < 2;
    const floors = stationary
      ? { looks: MIN_STATIONARY_LOOKS, says: MIN_STATIONARY_SAYS, moves: 0 }
      : {
          looks: MIN_FLEET_LOOKS,
          says: MIN_FLEET_SAYS,
          moves: MIN_FLEET_MOVES,
        };
    check(
      run,
      state.stats.looks >= floors.looks,
      `${state.name}: only ${state.stats.looks} successful looks, expected at least ${floors.looks}`,
    );
    check(
      run,
      state.stats.says >= floors.says,
      `${state.name}: only ${state.stats.says} successful says, expected at least ${floors.says}`,
    );
    if (!stationary) {
      check(
        run,
        state.stats.moves >= floors.moves,
        `${state.name}: only ${state.stats.moves} successful moves, expected at least ${floors.moves}`,
      );
    }
  }
  check(
    run,
    elapsedMs < SCRIPT_BUDGET_MS,
    `scenario elapsed ${formatMs(elapsedMs)} ms exceeds ${SCRIPT_BUDGET_MS} ms`,
  );
}

export function printResult(failureCount: number): void {
  write(
    failureCount === 0
      ? 'result: PASS'
      : `result: FAIL (${failureCount} failure(s))`,
  );
}
