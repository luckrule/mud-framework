import { readFileSync, writeFileSync } from 'node:fs';

import type {
  IpcChannel,
  IpcMessage,
  LogFields,
  Logger,
  LogLevel,
} from 'mud-framework-core';

import {
  isRecord,
  messageOf,
} from '../../../packages/core/src/shared/guards.ts';
import { Histogram, parseHistogramJson } from './support.ts';
import type { HistogramJson } from './support.ts';

/**
 * Benchmark-local Server instrumentation. `bench-server.ts` wraps the real
 * process channel in `createCountingChannel` and hands `runServerProcess` a
 * logger that records `request completed` durations for the sender sessions.
 * Nothing here is imported by production code or changes the Server hot path.
 */

export interface ServerIpcStats {
  readonly outboundFrames: number;
  readonly outboundBytes: number;
  readonly inboundFrames: number;
  readonly inboundBytes: number;
}

export interface ServerHandlerStats {
  readonly completed: number;
  readonly failed: number;
}

export interface ServerStatsJson {
  readonly version: 1;
  readonly pid: number;
  readonly ipc: ServerIpcStats;
  readonly handler: {
    readonly completed: number;
    readonly failed: number;
    /** `durationMs` histogram, stored as ns so it reuses the load buckets. */
    readonly durationNs: HistogramJson;
  };
}

export interface MutableIpcStats {
  outboundFrames: number;
  outboundBytes: number;
  inboundFrames: number;
  inboundBytes: number;
}

export interface MutableHandlerStats {
  completed: number;
  failed: number;
}

function frameBytes(value: unknown): number {
  try {
    const text = JSON.stringify(value);
    return typeof text === 'string' ? Buffer.byteLength(text) : 0;
  } catch {
    // A frame that cannot be measured is still a frame; count it as zero bytes
    // rather than failing the Server on a metric.
    return 0;
  }
}

/**
 * Counts outbound frames (results, events, barriers) and inbound frames
 * (requests, cancels, handshake) with their JSON wire size. `send` is counted
 * only after it succeeds, so a closed channel does not inflate the traffic.
 */
export function createCountingChannel(
  inner: IpcChannel,
  ipc: MutableIpcStats,
): IpcChannel {
  return {
    send(message: IpcMessage): void {
      const bytes = frameBytes(message);
      inner.send(message);
      ipc.outboundFrames += 1;
      ipc.outboundBytes += bytes;
    },
    onMessage(handler: (message: unknown) => void): void {
      inner.onMessage((raw) => {
        ipc.inboundFrames += 1;
        ipc.inboundBytes += frameBytes(raw);
        handler(raw);
      });
    },
    onClose(handler: (reason: string) => void): void {
      inner.onClose(handler);
    },
    close(reason?: string): void {
      inner.close(reason);
    },
  };
}

class RecordingLogger implements Logger {
  private readonly inner: Logger;
  private readonly senders: ReadonlySet<string>;
  private readonly handler: MutableHandlerStats;
  private readonly histogram: Histogram;

  constructor(
    inner: Logger,
    senders: ReadonlySet<string>,
    handler: MutableHandlerStats,
    histogram: Histogram,
  ) {
    this.inner = inner;
    this.senders = senders;
    this.handler = handler;
    this.histogram = histogram;
  }

  debug(fields: LogFields, message: string): void {
    this.record(fields, message);
    this.inner.debug(fields, message);
  }

  info(fields: LogFields, message: string): void {
    this.record(fields, message);
    this.inner.info(fields, message);
  }

  warn(fields: LogFields, message: string): void {
    this.record(fields, message);
    this.inner.warn(fields, message);
  }

  error(fields: LogFields, message: string): void {
    this.record(fields, message);
    this.inner.error(fields, message);
  }

  child(bindings: LogFields): Logger {
    return new RecordingLogger(
      this.inner.child(bindings),
      this.senders,
      this.handler,
      this.histogram,
    );
  }

  setLevel(level: LogLevel): void {
    this.inner.setLevel(level);
  }

  private record(fields: LogFields, message: string): void {
    if (message !== 'request completed' && message !== 'request failed') {
      return;
    }
    if (!this.isSenderSession(fields)) {
      return;
    }
    if (message === 'request failed') {
      this.handler.failed += 1;
      return;
    }
    this.handler.completed += 1;
    const durationMs = fields.durationMs;
    if (
      typeof durationMs === 'number' &&
      Number.isFinite(durationMs) &&
      durationMs >= 0
    ) {
      this.histogram.record(durationMs * 1e6);
    }
  }

  /**
   * `game.login` enters without a Session and only declares one through
   * `sessionChange`, so both fields identify a sender request.
   */
  private isSenderSession(fields: LogFields): boolean {
    const direct = fields.sessionId;
    if (typeof direct === 'string' && this.senders.has(direct)) {
      return true;
    }
    const change = fields.sessionChange;
    return (
      isRecord(change) &&
      typeof change.sessionId === 'string' &&
      this.senders.has(change.sessionId)
    );
  }
}

/**
 * Forwards to the real logger and records the outcome of requests that
 * entered with one of `senders`. Every level is inspected because a failed
 * request is logged through `failureLogLevel()` (warn/error), not debug. The
 * inner logger keeps its own level (info by default), so the debug lines are
 * filtered after recording and never reach stdout.
 */
export function createRecordingLogger(
  inner: Logger,
  senders: ReadonlySet<string>,
  handler: MutableHandlerStats,
  histogram: Histogram,
): Logger {
  return new RecordingLogger(inner, senders, handler, histogram);
}

export function buildServerStats(
  ipc: MutableIpcStats,
  handler: MutableHandlerStats,
  histogram: Histogram,
): ServerStatsJson {
  return {
    version: 1,
    pid: process.pid,
    ipc: { ...ipc },
    handler: {
      completed: handler.completed,
      failed: handler.failed,
      durationNs: histogram.toJson(),
    },
  };
}

export function writeServerStats(path: string, stats: ServerStatsJson): void {
  writeFileSync(path, JSON.stringify(stats));
}

export type ServerStatsRead =
  | { readonly ok: true; readonly stats: ServerStatsJson }
  | { readonly ok: false; readonly reason: string };

export function scanSenderSessions(
  raw: string | undefined,
): ReadonlySet<string> {
  if (raw === undefined) {
    return new Set();
  }
  return new Set(
    raw
      .split(',')
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0),
  );
}

function integerField(
  record: Record<string, unknown>,
  field: string,
  label: string,
): number {
  const value = record[field];
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
    throw new Error(`${label}.${field} is not a non-negative integer`);
  }
  return value;
}

function parseStats(value: unknown): ServerStatsJson {
  if (!isRecord(value) || value.version !== 1) {
    throw new Error('server stats is not a version 1 object');
  }
  if (typeof value.pid !== 'number' || !Number.isInteger(value.pid)) {
    throw new Error('server stats pid is not an integer');
  }
  const ipcRaw = value.ipc;
  const handlerRaw = value.handler;
  if (!isRecord(ipcRaw) || !isRecord(handlerRaw)) {
    throw new Error('server stats is missing its ipc or handler section');
  }
  return {
    version: 1,
    pid: value.pid,
    ipc: {
      outboundFrames: integerField(ipcRaw, 'outboundFrames', 'ipc'),
      outboundBytes: integerField(ipcRaw, 'outboundBytes', 'ipc'),
      inboundFrames: integerField(ipcRaw, 'inboundFrames', 'ipc'),
      inboundBytes: integerField(ipcRaw, 'inboundBytes', 'ipc'),
    },
    handler: {
      completed: integerField(handlerRaw, 'completed', 'handler'),
      failed: integerField(handlerRaw, 'failed', 'handler'),
      durationNs: parseHistogramJson(
        handlerRaw.durationNs,
        'handler.durationNs',
      ),
    },
  };
}

export function readServerStatsFile(path: string): ServerStatsRead {
  try {
    return {
      ok: true,
      stats: parseStats(JSON.parse(readFileSync(path, 'utf8'))),
    };
  } catch (error) {
    return { ok: false, reason: messageOf(error) };
  }
}
