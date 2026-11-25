import { randomUUID } from 'node:crypto';

import { AUTH_REQUIRED } from 'mud-framework-core';

const DEFAULT_PORTAL_PORT = 8080;
const REQUEST_TIMEOUT_MS = 5000;
const PLAYER_NAME = process.env.PLAYER_NAME ?? 'demo';

interface OkResponse {
  readonly id: string;
  readonly ok: true;
  readonly result: unknown;
}

interface FailureResponse {
  readonly id: string;
  readonly ok: false;
  readonly error: { readonly code: string; readonly message: string };
}

type WireResponse = OkResponse | FailureResponse;

interface PendingRequest {
  readonly resolve: (response: WireResponse) => void;
  readonly reject: (error: Error) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function readPort(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > 65535) {
    throw new Error(
      `${name} must be an integer between 1 and 65535, got ${JSON.stringify(raw)}`,
    );
  }
  return value;
}

function write(line: string): void {
  process.stdout.write(`${line}\n`);
}

function writeError(line: string): void {
  process.stderr.write(`${line}\n`);
}

function show(value: unknown): string {
  return JSON.stringify(value) ?? String(value);
}

/** The wire response is `{id} & ({ok: true, result} | {ok: false, error})`. */
function parseResponse(raw: string): WireResponse {
  const parsed: unknown = JSON.parse(raw);
  if (
    !isRecord(parsed) ||
    typeof parsed.id !== 'string' ||
    typeof parsed.ok !== 'boolean'
  ) {
    throw new Error('malformed response envelope');
  }
  if (parsed.ok) {
    return { id: parsed.id, ok: true, result: parsed.result };
  }
  const error = parsed.error;
  if (
    !isRecord(error) ||
    typeof error.code !== 'string' ||
    typeof error.message !== 'string'
  ) {
    throw new Error('malformed error response');
  }
  return {
    id: parsed.id,
    ok: false,
    error: { code: error.code, message: error.message },
  };
}

function readResultString(result: unknown, field: string): string {
  const value = isRecord(result) ? result[field] : undefined;
  if (typeof value !== 'string') {
    throw new Error(`expected a string result.${field}, got ${show(result)}`);
  }
  return value;
}

function requireOk(response: WireResponse, method: string): OkResponse {
  if (!response.ok) {
    throw new Error(
      `${method} failed: ${response.error.code}: ${response.error.message}`,
    );
  }
  return response;
}

async function main(): Promise<void> {
  const port = readPort('PORTAL_PORT', DEFAULT_PORTAL_PORT);
  const url = `ws://127.0.0.1:${port}/`;
  const socket = new WebSocket(url);
  const pending = new Map<string, PendingRequest>();

  const rejectPending = (error: Error): void => {
    for (const request of pending.values()) {
      clearTimeout(request.timer);
      request.reject(error);
    }
    pending.clear();
  };

  const closed = new Promise<void>((resolve) => {
    socket.addEventListener('close', () => resolve(), { once: true });
  });

  socket.addEventListener('message', (event) => {
    const raw = typeof event.data === 'string' ? event.data : undefined;
    if (raw === undefined) {
      rejectPending(new Error('received a non-text frame'));
      return;
    }
    let response: WireResponse;
    try {
      response = parseResponse(raw);
    } catch (error) {
      rejectPending(error instanceof Error ? error : new Error(String(error)));
      return;
    }
    const request = pending.get(response.id);
    if (request === undefined) {
      return;
    }
    pending.delete(response.id);
    clearTimeout(request.timer);
    request.resolve(response);
  });
  socket.addEventListener('error', () => {
    rejectPending(new Error('WebSocket error'));
  });

  const request = (
    method: string,
    payload?: unknown,
  ): Promise<WireResponse> => {
    const id = randomUUID();
    return new Promise<WireResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(id);
        reject(new Error(`${method} timed out after ${REQUEST_TIMEOUT_MS}ms`));
      }, REQUEST_TIMEOUT_MS);
      pending.set(id, { resolve, reject, timer });
      socket.send(JSON.stringify({ id, method, payload }));
    });
  };

  try {
    await new Promise<void>((resolve, reject) => {
      socket.addEventListener('open', () => resolve(), { once: true });
      socket.addEventListener(
        'error',
        () => reject(new Error(`could not connect to ${url}`)),
        { once: true },
      );
    });
    write(`client: connected to ${url}`);

    const login = requireOk(
      await request('player.login', { name: PLAYER_NAME }),
      'player.login',
    );
    const sessionId = readResultString(login.result, 'sessionId');
    write(`client: login -> ${show(login.result)}`);

    const whoami = requireOk(await request('player.whoami'), 'player.whoami');
    if (readResultString(whoami.result, 'sessionId') !== sessionId) {
      throw new Error('player.whoami reported a different Session id');
    }
    write(`client: whoami -> ${show(whoami.result)}`);

    const logout = requireOk(await request('player.logout'), 'player.logout');
    write(`client: logout -> ${show(logout.result)}`);

    // The Session gate is in the business process (SPEC 3.7): after a
    // successful logout the connection is unbound and this must be refused.
    const anonymous = await request('player.whoami');
    if (anonymous.ok) {
      throw new Error('player.whoami succeeded after logout');
    }
    if (anonymous.error.code !== AUTH_REQUIRED) {
      throw new Error(
        `expected ${AUTH_REQUIRED} after logout, got ${anonymous.error.code}`,
      );
    }
    write(
      `client: whoami after logout -> ${anonymous.error.code}: ${anonymous.error.message}`,
    );
    write('client: sequence completed');
  } finally {
    rejectPending(new Error('client finished'));
    if (
      socket.readyState === WebSocket.OPEN ||
      socket.readyState === WebSocket.CONNECTING
    ) {
      socket.close(1000, 'client finished');
      await Promise.race([closed, Bun.sleep(1000)]);
    }
  }
}

try {
  await main();
} catch (error) {
  writeError(`client failed: ${messageOf(error)}`);
  process.exitCode = 1;
}
