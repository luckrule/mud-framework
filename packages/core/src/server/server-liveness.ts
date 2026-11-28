/**
 * How often the Server checks whether Portal is still alive. `process.ppid`
 * changing means the parent died and this process was reparented; the IPC
 * `disconnect` event does not fire for an abrupt parent death, so without this
 * poll an orphan could keep running (and writing) indefinitely.
 */
const PARENT_CHECK_INTERVAL_MS = 500;

/** Test-injected knobs; a deployment uses the defaults in this module. */
export interface ParentWatchOptions {
  readonly parentCheckIntervalMs?: number;
  readonly readParentPid?: () => number;
}

/**
 * Watches for the Portal dying without closing the link. `process.ppid` is
 * read on an interval because an abrupt parent death (SIGKILL) reparents this
 * process without firing `disconnect`; without the poll the orphan would keep
 * running and, worse, keep writing to storage beside the next Server
 * (INV-002). The timer is unref'd so it never keeps the process alive.
 */
export class ParentWatch {
  private timer: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly options: ParentWatchOptions,
    private readonly onGone: () => void,
  ) {}

  start(): void {
    const intervalMs =
      this.options.parentCheckIntervalMs ?? PARENT_CHECK_INTERVAL_MS;
    if (intervalMs <= 0) {
      return;
    }
    const readParentPid = this.options.readParentPid ?? (() => process.ppid);
    const parentPid = readParentPid();
    this.timer = setInterval(() => {
      if (readParentPid() === parentPid) {
        return;
      }
      this.stop();
      this.onGone();
    }, intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer === undefined) {
      return;
    }
    clearInterval(this.timer);
    this.timer = undefined;
  }
}
