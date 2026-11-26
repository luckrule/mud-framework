/**
 * Shutdown order for the minimal Portal, kept separate from `portal.ts` so the
 * failure path is testable without binding ports.
 *
 * A rejected `layer.stop()` must not skip `health.stop(true)`: the HTTP
 * listener would keep the event loop alive after SIGTERM. Both stops are
 * therefore always attempted, and failures are reported in stop order so a
 * later health error cannot mask the layer error.
 */

export interface ShutdownSteps {
  readonly stopLayer: () => Promise<void>;
  readonly stopHealth: () => Promise<void>;
}

export type ShutdownTarget = 'layer' | 'health';

export interface ShutdownFailure {
  readonly target: ShutdownTarget;
  readonly error: unknown;
}

export interface ShutdownOutcome {
  /** 0 only when both stops succeeded. */
  readonly exitCode: 0 | 1;
  /** Failed stops in stop order; a layer error precedes a health error. */
  readonly failures: readonly ShutdownFailure[];
}

export async function shutdownPortal(
  steps: ShutdownSteps,
): Promise<ShutdownOutcome> {
  const failures: ShutdownFailure[] = [];
  try {
    await steps.stopLayer();
  } catch (error) {
    failures.push({ target: 'layer', error });
  }
  try {
    await steps.stopHealth();
  } catch (error) {
    failures.push({ target: 'health', error });
  }
  return {
    exitCode: failures.length === 0 ? 0 : 1,
    failures,
  };
}
