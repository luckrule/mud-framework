/**
 * Cleanup decision shared by the benchmarks that create their own data
 * directory. A surviving Portal or Server may still have the SQLite file open,
 * so deleting the directory before every process is confirmed gone both hides
 * the failure scene and can unlink a database a live writer still uses.
 */

export interface DataDirCleanupState {
  /** Stopping threw, exited non-zero, was forced, or never confirmed an exit. */
  readonly stopFailed: boolean;
  /** Child pids still alive after the bounded stop wait. */
  readonly survivors: readonly number[];
  /** The directory is user-owned or the user asked to keep it. */
  readonly keep: boolean;
}

export function shouldRemoveDataDir(state: DataDirCleanupState): boolean {
  return !state.keep && !state.stopFailed && state.survivors.length === 0;
}

export interface PortalStopOutcome {
  /** From the child exit event; undefined means the bounded wait saw no exit. */
  readonly exitCode: number | null | undefined;
  /** The SIGKILL fallback ran, or an orphaned Server had to be reaped. */
  readonly forced: boolean;
  /** Child pids still alive after the bounded stop wait. */
  readonly survivors: readonly number[];
}

/**
 * Whether the Portal stop was too abnormal to remove the data directory.
 *
 * `forced` is non-graceful by definition: the Portal needed SIGKILL, or a
 * Server outlived it and had to be reaped. Keeping the scene is the point, so
 * it counts even when nothing survived. A signal exit (`exitCode === null`)
 * with no survivors is still an observed exit — the bounded wait saw the
 * process go — and is treated as gone; only an exit the wait never observed
 * (`undefined`), a non-zero code, or a survivor keeps the directory.
 */
export function portalStopFailed(outcome: PortalStopOutcome): boolean {
  const badExit =
    outcome.exitCode !== undefined &&
    outcome.exitCode !== null &&
    outcome.exitCode !== 0;
  return (
    outcome.forced ||
    outcome.exitCode === undefined ||
    badExit ||
    outcome.survivors.length > 0
  );
}
