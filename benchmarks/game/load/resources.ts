import { readFileSync, readdirSync } from 'node:fs';

import type { PortalHandle } from '../support.ts';

/**
 * Per-second RSS and fd traces for the Portal and Server. All of it reads
 * `/proc`, so sampling is skipped on platforms without it; a process that
 * disappears between the liveness check and the read is a miss, not an error.
 */

const SAMPLE_INTERVAL_MS = 1000;

export interface PidTraceReport {
  readonly label: string;
  readonly samples: number;
  readonly misses: number;
  readonly peakRssKb: number | undefined;
  readonly lastRssKb: number | undefined;
  readonly peakFds: number | undefined;
  readonly lastFds: number | undefined;
}

class PidTrace {
  private sampleCount = 0;
  private missCount = 0;
  private peakRssKb: number | undefined;
  private lastRssKb: number | undefined;
  private peakFds: number | undefined;
  private lastFds: number | undefined;
  readonly label: string;

  constructor(label: string) {
    this.label = label;
  }

  sample(pid: number | undefined): void {
    if (pid === undefined) {
      this.missCount += 1;
      return;
    }
    const rssKb = readRssKb(pid);
    const fds = readFdCount(pid);
    if (rssKb === undefined && fds === undefined) {
      this.missCount += 1;
      return;
    }
    this.sampleCount += 1;
    if (rssKb !== undefined) {
      this.lastRssKb = rssKb;
      this.peakRssKb =
        this.peakRssKb === undefined ? rssKb : Math.max(this.peakRssKb, rssKb);
    }
    if (fds !== undefined) {
      this.lastFds = fds;
      this.peakFds =
        this.peakFds === undefined ? fds : Math.max(this.peakFds, fds);
    }
  }

  report(): PidTraceReport {
    return {
      label: this.label,
      samples: this.sampleCount,
      misses: this.missCount,
      peakRssKb: this.peakRssKb,
      lastRssKb: this.lastRssKb,
      peakFds: this.peakFds,
      lastFds: this.lastFds,
    };
  }
}

function readRssKb(pid: number): number | undefined {
  try {
    const status = readFileSync(`/proc/${pid}/status`, 'utf8');
    const match = /^VmRSS:\s+(\d+)\s+kB$/m.exec(status);
    return match === null ? undefined : Number(match[1]);
  } catch {
    // The process exited between sampling the pid and reading /proc.
    return undefined;
  }
}

function readFdCount(pid: number): number | undefined {
  try {
    return readdirSync(`/proc/${pid}/fd`).length;
  } catch {
    return undefined;
  }
}

export interface ResourceReport {
  readonly supported: boolean;
  readonly portalPid: number | undefined;
  readonly portal: PidTraceReport;
  readonly serverPid: number | undefined;
  readonly serverSeen: boolean;
  readonly server: PidTraceReport;
}

export class ResourceSampler {
  private readonly portalTrace = new PidTrace('portal');
  private readonly serverTrace = new PidTrace('server');
  private readonly portal: PortalHandle | undefined;
  private readonly portalPid: number | undefined;
  private timer: ReturnType<typeof setInterval> | undefined;
  private serverPid: number | undefined;
  private serverSeen = false;

  constructor(portal: PortalHandle | undefined) {
    this.portal = portal;
    this.portalPid = portal?.pid;
  }

  start(): void {
    if (process.platform !== 'linux') {
      return;
    }
    this.tick();
    this.timer = setInterval(() => {
      this.tick();
    }, SAMPLE_INTERVAL_MS);
  }

  stop(): ResourceReport {
    if (this.timer !== undefined) {
      clearInterval(this.timer);
      this.timer = undefined;
      this.tick();
    }
    return {
      supported: process.platform === 'linux',
      portalPid: this.portalPid,
      portal: this.portalTrace.report(),
      serverPid: this.serverPid,
      serverSeen: this.serverSeen,
      server: this.serverTrace.report(),
    };
  }

  private tick(): void {
    this.portalTrace.sample(this.portalPid);
    // The Server logs `server process ready`; follow the newest live pid in
    // case a restart ever swaps it during the run.
    const pid = this.portal?.liveServerPid();
    if (pid !== undefined) {
      this.serverPid = pid;
      this.serverSeen = true;
    }
    this.serverTrace.sample(pid);
  }
}
