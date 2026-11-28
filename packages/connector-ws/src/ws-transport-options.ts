import type { Logger } from 'mud-framework-core';

import { assertPositiveInteger } from './send-queue.ts';

export interface WsTransportOptions {
  /**
   * Listening port; `0` (the default) lets the OS pick one. Must be an
   * integer between 0 and 65535.
   */
  port?: number;
  /** Must be a non-empty string. */
  hostname?: string;
  /**
   * Only upgrade requests for this path are accepted. Must be a non-empty
   * string starting with `/`.
   */
  path?: string;
  /** Per-message ceiling for both directions. Must be a positive integer. */
  maxMessageBytes?: number;
  /**
   * Forwarded to Bun's WebSocket `idleTimeout`, in seconds. Must be a
   * non-negative integer; `0` disables the timeout.
   */
  idleTimeoutSeconds?: number;
  /**
   * How long a payload may wait behind a full socket buffer before its
   * `send` rejects and the connection is dropped. `Infinity` disables it.
   * Must be a positive finite number or `Infinity`.
   */
  sendTimeoutMs?: number;
  /**
   * Frames that may wait unsettled in one connection's outbound queue before
   * it is failed. Must be a positive integer; defaults to 1024.
   */
  maxQueuedFrames?: number;
  /**
   * Unsettled outbound bytes per connection before it is failed. Must be a
   * positive integer no smaller than `maxMessageBytes`, so one legal
   * maximum-size frame always fits; defaults to 4 MiB, raised to
   * `maxMessageBytes` when that is larger.
   */
  maxQueuedBytes?: number;
  /**
   * Optional: this package depends on core's types and the stateless
   * `safeLogger` wrapper only, never on a logger implementation; the caller
   * supplies the implementation. Without it nothing is logged.
   */
  logger?: Logger;
}

export interface WsTransportSettings {
  readonly port: number;
  readonly hostname: string;
  readonly path: string;
  readonly maxMessageBytes: number;
  readonly idleTimeoutSeconds: number;
  readonly sendTimeoutMs: number;
  readonly maxQueuedFrames: number;
  readonly maxQueuedBytes: number;
}

const DEFAULT_MAX_MESSAGE_BYTES = 1024 * 1024;
const DEFAULT_IDLE_TIMEOUT_SECONDS = 30;
const DEFAULT_SEND_TIMEOUT_MS = 30_000;
/**
 * The input queue bounds a connection at 1000 waiting requests, so one
 * response per request must fit. 1024 lets a refusal storm finish without
 * letting a client that never reads pin memory without bound.
 */
const DEFAULT_MAX_QUEUED_FRAMES = 1024;
/**
 * 4 MiB holds an ordinary burst (1000 refusal frames are ~80 KB) and several
 * maximum-size frames. The effective default is raised to `maxMessageBytes`
 * when a caller allows larger single frames.
 */
const DEFAULT_MAX_QUEUED_BYTES = 4 * 1024 * 1024;

export function resolveWsTransportSettings(
  options: WsTransportOptions,
): WsTransportSettings {
  const port = options.port ?? 0;
  assertIntegerBetween(port, 'port', 0, 65_535);
  const maxMessageBytes = options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES;
  assertPositiveInteger(maxMessageBytes, 'maxMessageBytes');
  const idleTimeoutSeconds =
    options.idleTimeoutSeconds ?? DEFAULT_IDLE_TIMEOUT_SECONDS;
  assertNonNegativeInteger(idleTimeoutSeconds, 'idleTimeoutSeconds');
  const sendTimeoutMs = options.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS;
  assertSendTimeout(sendTimeoutMs, 'sendTimeoutMs');
  const maxQueuedFrames = options.maxQueuedFrames ?? DEFAULT_MAX_QUEUED_FRAMES;
  assertPositiveInteger(maxQueuedFrames, 'maxQueuedFrames');
  const maxQueuedBytes =
    options.maxQueuedBytes ??
    Math.max(DEFAULT_MAX_QUEUED_BYTES, maxMessageBytes);
  assertPositiveInteger(maxQueuedBytes, 'maxQueuedBytes');
  if (maxQueuedBytes < maxMessageBytes) {
    // A queue smaller than one legal frame would fail that frame on arrival
    // and drop the connection, so the limit could never be reached legally.
    throw new RangeError(
      `maxQueuedBytes (${maxQueuedBytes}) must be at least maxMessageBytes (${maxMessageBytes}), or a maximum-size frame overflows the queue`,
    );
  }
  const hostname = options.hostname ?? '127.0.0.1';
  assertNonEmptyString(hostname, 'hostname');
  const path = options.path ?? '/';
  assertPath(path);
  return {
    port,
    hostname,
    path,
    maxMessageBytes,
    idleTimeoutSeconds,
    sendTimeoutMs,
    maxQueuedFrames,
    maxQueuedBytes,
  };
}

function assertIntegerBetween(
  value: unknown,
  name: string,
  min: number,
  max: number,
): void {
  const expected = `an integer between ${min} and ${max}`;
  if (typeof value !== 'number') {
    throw new TypeError(`${name} must be ${expected}, got ${String(value)}`);
  }
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new RangeError(`${name} must be ${expected}, got ${String(value)}`);
  }
}

function assertNonEmptyString(value: unknown, name: string): string {
  if (typeof value !== 'string') {
    throw new TypeError(
      `${name} must be a non-empty string, got ${String(value)}`,
    );
  }
  if (value.length === 0) {
    throw new RangeError(`${name} must be a non-empty string, got ""`);
  }
  return value;
}

function assertPath(value: unknown): void {
  const path = assertNonEmptyString(value, 'path');
  if (!path.startsWith('/')) {
    throw new RangeError(
      `path must start with "/", got ${JSON.stringify(path)}`,
    );
  }
}

function assertSendTimeout(value: unknown, name: string): void {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a positive number or Infinity, got ${String(value)}`,
    );
  }
  const valid =
    value === Number.POSITIVE_INFINITY || (Number.isFinite(value) && value > 0);
  if (!valid) {
    throw new RangeError(
      `${name} must be a positive number or Infinity, got ${String(value)}`,
    );
  }
}

function assertNonNegativeInteger(value: unknown, name: string): void {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a non-negative integer, got ${String(value)}`,
    );
  }
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(
      `${name} must be a non-negative integer, got ${String(value)}`,
    );
  }
}
