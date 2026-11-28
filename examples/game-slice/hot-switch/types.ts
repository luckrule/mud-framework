import type { GameClient, GameEvent } from '../src/client-lib.ts';
import type { GameVersion } from '../src/game.ts';
import type { RunningPortal } from './portal.ts';
import type { SerialQueue } from './support.ts';

/** Shared shapes: per-client bookkeeping, one switch window, the whole run. */

export interface AttemptStats {
  attempts: number;
  ok: number;
  clientTimeouts: number;
  frameworkTimeouts: number;
  failures: number;
  disconnects: number;
  retried: number;
  retrySucceeded: number;
  recoveredMoves: number;
  /** Successful commands by method, for the load-activity floors. */
  looks: number;
  says: number;
  moves: number;
  /** Every attempt, failed and timed-out ones included. */
  readonly latencyNs: number[];
  readonly codes: Map<string, number>;
  /** Repeats keyed by the window the first attempt belonged to. */
  readonly repeatsByWindow: Map<number, number>;
  readonly firstAttemptWindow: Map<string, number>;
}

export interface DrillClient {
  readonly index: number;
  readonly name: string;
  readonly client: GameClient;
  readonly stats: AttemptStats;
  readonly queue: SerialQueue;
  sessionId: string;
  room: string;
}

export interface RequestSample {
  readonly startNs: number;
  readonly endNs: number;
  readonly ok: boolean;
}

export interface EventSample {
  readonly sentAtNs: number;
  readonly atNs: number;
  readonly latencyNs: number;
  readonly marker: string;
  readonly text: string;
}

export interface SwitchWindow {
  readonly index: number;
  readonly target: GameVersion;
  readonly startNs: number;
  readonly endNs: number;
}

export interface SwitchReport {
  readonly label: string;
  readonly target: GameVersion;
  readonly controlMs: number;
  readonly wallMs: number;
  readonly fromPid: number | undefined;
  readonly toPid: number | undefined;
  readonly oldExited: boolean;
  readonly oldExitWaitMs: number;
  readonly newAlive: boolean;
  readonly versionText: string;
  readonly crossText: string;
  readonly crossEventMs: number | undefined;
  readonly ordinaryText: string;
  readonly ordinaryEventMs: number | undefined;
}

export interface StateCheck {
  readonly label: string;
  readonly itemIds: readonly string[];
  readonly room: string;
  readonly you: string;
  readonly sessionId: string;
}

/**
 * A one-shot nudge for the anchor's next sleep, so a regular load `say` can be
 * submitted inside the switch window instead of depending on where its normal
 * cadence happened to land.
 */
export interface WindowWake {
  fired: boolean;
}

export interface Run {
  readonly runTag: string;
  readonly dataDirectory: string;
  readonly portal: RunningPortal;
  readonly clients: DrillClient[];
  readonly failures: string[];
  readonly requestSamples: RequestSample[];
  readonly eventSamples: EventSample[];
  readonly windows: SwitchWindow[];
  readonly switches: SwitchReport[];
  readonly stateChecks: StateCheck[];
  /** Send times of the anchor's says, so the watcher can compute latency. */
  readonly anchorSendTimes: Map<string, number>;
  /** Markers whose say command succeeded, and those observed as events. */
  readonly anchorExpected: Set<string>;
  readonly anchorObserved: Set<string>;
  activeSwitch: number;
  probes: number;
  stopping: boolean;
  windowWake: WindowWake | undefined;
}

export type CommandResult =
  | { readonly ok: true; readonly result: unknown; readonly attempts: number }
  | {
      readonly ok: false;
      readonly code: string;
      readonly message: string;
      readonly attempts: number;
    };

export interface ControlReply {
  readonly durationMs: number;
  readonly fromPid: number | undefined;
  readonly toPid: number | undefined;
}

export interface CrossSwitchProbe {
  readonly marker: string;
  readonly said: Promise<boolean>;
  readonly observed: Promise<GameEvent>;
}
