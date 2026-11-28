import { describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { isProcessAlive, sleep } from 'mud-framework-test-support';

/**
 * The Portal's loopback control endpoint: off unless asked for, and its
 * refusals are the contract the drill relies on.
 */
const WORKSPACE = join(import.meta.dir, '..', '..');
const PORTAL = join(WORKSPACE, 'examples', 'game-slice', 'src', 'portal.ts');
const READY_TIMEOUT_MS = 20_000;
const EXIT_TIMEOUT_MS = 20_000;
const POLL_INTERVAL_MS = 25;

interface PortalRun {
  readonly controlUrl: string | undefined;
  readonly pids: readonly number[];
  stop(): Promise<void>;
}

async function waitForExit(
  child: Bun.Subprocess,
  timeoutMs: number,
): Promise<number | null | undefined> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<undefined>((resolve) => {
    timer = setTimeout(() => {
      resolve(undefined);
    }, timeoutMs);
  });
  try {
    return await Promise.race([child.exited, timeout]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

function pidOf(line: string): number | undefined {
  try {
    const parsed: unknown = JSON.parse(line);
    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      'msg' in parsed &&
      (parsed.msg === 'server ready' ||
        parsed.msg === 'replacement server ready') &&
      'pid' in parsed &&
      typeof parsed.pid === 'number'
    ) {
      return parsed.pid;
    }
  } catch {
    return undefined;
  }
  return undefined;
}

async function startPortal(
  dataDirectory: string,
  control: boolean,
): Promise<PortalRun> {
  const child = Bun.spawn({
    cmd: [process.execPath, 'run', PORTAL],
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'inherit',
    env: {
      ...process.env,
      PORTAL_PORT: '0',
      GAME_SLICE_DATA_DIR: dataDirectory,
      ...(control ? { GAME_SLICE_CONTROL: '1' } : { GAME_SLICE_CONTROL: '' }),
    },
  });
  const reader = child.stdout.getReader();
  const decoder = new TextDecoder();
  const pids: number[] = [];
  let pending = '';
  let controlUrl: string | undefined;
  let listening = false;
  const readyDeadline = Date.now() + READY_TIMEOUT_MS;
  while (Date.now() < readyDeadline) {
    const chunk = await reader.read();
    if (chunk.done) {
      break;
    }
    pending += decoder.decode(chunk.value, { stream: true });
    const parts = pending.split('\n');
    pending = parts.pop() ?? '';
    for (const part of parts) {
      const pid = pidOf(part);
      if (pid !== undefined) {
        pids.push(pid);
      }
      if (part.startsWith('portal: listening on ')) {
        listening = true;
      }
      const match = /^portal: control on (http:\/\/\S+)$/.exec(part);
      if (match !== null) {
        controlUrl = match[1];
      }
    }
    if (listening && (controlUrl !== undefined || !control)) {
      break;
    }
  }
  // Keep draining so the child never blocks on a full stdout pipe.
  void (async () => {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) {
        return;
      }
    }
  })().catch(() => undefined);

  let stopped: Promise<void> | undefined;
  return {
    controlUrl,
    pids,
    stop: () => {
      stopped ??= (async () => {
        child.kill('SIGTERM');
        const code = await waitForExit(child, EXIT_TIMEOUT_MS);
        if (code === undefined) {
          child.kill('SIGKILL');
          await waitForExit(child, EXIT_TIMEOUT_MS);
        }
        const exitDeadline = Date.now() + EXIT_TIMEOUT_MS;
        while (pids.some(isProcessAlive) && Date.now() < exitDeadline) {
          await sleep(POLL_INTERVAL_MS);
        }
        for (const pid of pids) {
          if (isProcessAlive(pid)) {
            process.kill(pid, 'SIGKILL');
          }
        }
      })();
      return stopped;
    },
  };
}

function post(url: string, body: string): Promise<Response> {
  return fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body,
  });
}

describe('game-slice control endpoint', () => {
  test('is off by default', async () => {
    const dataDirectory = mkdtempSync(join(tmpdir(), 'game-slice-control-'));
    const portal = await startPortal(dataDirectory, false);
    try {
      expect(portal.controlUrl).toBeUndefined();
    } finally {
      await portal.stop();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  }, 60_000);

  test('refuses unknown paths, methods and bodies, and serialises switches', async () => {
    const dataDirectory = mkdtempSync(join(tmpdir(), 'game-slice-control-'));
    const portal = await startPortal(dataDirectory, true);
    const url = portal.controlUrl;
    expect(url).toBeDefined();
    if (url === undefined) {
      await portal.stop();
      rmSync(dataDirectory, { recursive: true, force: true });
      return;
    }
    try {
      expect(url).toStartWith('http://127.0.0.1:');
      const other = url.replace(/\/switch$/, '/other');
      expect((await fetch(other)).status).toBe(404);
      expect((await fetch(url)).status).toBe(405);
      expect((await post(url, 'not json')).status).toBe(400);
      expect((await post(url, '{"version":"v9"}')).status).toBe(400);

      // Five concurrent switches: one wins, the rest are refused.
      const statuses = await Promise.all(
        Array.from({ length: 5 }, () => post(url, '{"version":"v2"}')),
      );
      const codes = statuses
        .map((response) => response.status)
        .toSorted((left, right) => left - right);
      expect(codes.filter((code) => code === 200)).toHaveLength(1);
      expect(codes.filter((code) => code === 409)).toHaveLength(4);
      const winner = statuses.find((response) => response.status === 200);
      const body: unknown = await winner?.json();
      expect(body).toMatchObject({ ok: true });
    } finally {
      await portal.stop();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  }, 60_000);
});
