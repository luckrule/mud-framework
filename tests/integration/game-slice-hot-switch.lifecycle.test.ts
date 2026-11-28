import { describe, expect, test } from 'bun:test';
import { mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { connect } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { sleep } from 'mud-framework-test-support';

/**
 * The drill's lifecycle edges: a signal in the startup window (before the
 * Portal is ready) and a closed stdout must both end with no Portal, no
 * Server, no listening port and no leftover temporary directory.
 */
const WORKSPACE = join(import.meta.dir, '..', '..');
const DRILL = join(WORKSPACE, 'examples', 'game-slice', 'hot-switch.ts');
const PORTAL = join(WORKSPACE, 'examples', 'game-slice', 'src', 'portal.ts');
const TEMP_PREFIX = 'game-slice-hot-switch-';
const TAG_NAME = 'HOT_SWITCH_LIFECYCLE_TEST';
const EXIT_TIMEOUT_MS = 20_000;
const SETTLE_TIMEOUT_MS = 10_000;
const PORT_PROBE_TIMEOUT_MS = 500;
const POLL_INTERVAL_MS = 25;

interface ProcessEntry {
  readonly pid: number;
  readonly cmd: string;
}

/**
 * Only processes carrying this test run's tag: the integration suite runs in
 * parallel, and another test's real Portal/Server must not count as ours.
 * The tag is inherited by the drill's Portal and Server.
 */
function taggedProcesses(tag: string): ProcessEntry[] {
  const needle = `${TAG_NAME}=${tag}`;
  const found: ProcessEntry[] = [];
  let entries: string[];
  try {
    entries = readdirSync('/proc');
  } catch {
    return found;
  }
  for (const entry of entries) {
    if (!/^\d+$/.test(entry)) {
      continue;
    }
    const pid = Number(entry);
    if (pid === process.pid) {
      continue;
    }
    let environ: string;
    try {
      environ = readFileSync(`/proc/${entry}/environ`, 'utf8');
    } catch {
      continue;
    }
    if (!environ.includes(needle)) {
      continue;
    }
    let cmd = '';
    try {
      cmd = readFileSync(`/proc/${entry}/cmdline`, 'utf8')
        .replaceAll('\0', ' ')
        .trim();
    } catch {
      // The environment is what identified it; the cmdline is diagnostic only.
    }
    found.push({ pid, cmd });
  }
  return found;
}

function tempDirs(): Set<string> {
  return new Set(
    readdirSync(tmpdir()).filter((name) => name.startsWith(TEMP_PREFIX)),
  );
}

function listeningPorts(output: string): number[] {
  const ports = new Set<number>();
  for (const match of output.matchAll(
    /portal: listening on ws:\/\/127\.0\.0\.1:(\d+)\//g,
  )) {
    ports.add(Number(match[1]));
  }
  for (const match of output.matchAll(
    /portal: control on http:\/\/127\.0\.0\.1:(\d+)\/switch/g,
  )) {
    ports.add(Number(match[1]));
  }
  return [...ports];
}

function portOpen(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = connect({ host: '127.0.0.1', port });
    const finish = (open: boolean): void => {
      socket.destroy();
      resolve(open);
    };
    socket.setTimeout(PORT_PROBE_TIMEOUT_MS, () => finish(false));
    socket.once('connect', () => finish(true));
    socket.once('error', () => finish(false));
  });
}

async function readAll(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let output = '';
  for (;;) {
    const chunk = await reader.read();
    if (chunk.done) {
      return output;
    }
    output += decoder.decode(chunk.value, { stream: true });
  }
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

async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await sleep(POLL_INTERVAL_MS);
  }
  if (!predicate()) {
    throw new Error(`${label} did not settle within ${timeoutMs}ms`);
  }
}

async function expectNoLifecycleLeftovers(
  output: string,
  dirsBefore: ReadonlySet<string>,
  tag: string,
): Promise<void> {
  const leaked = [...tempDirs()].filter((name) => !dirsBefore.has(name));
  expect(leaked).toEqual([]);
  for (const port of listeningPorts(output)) {
    expect(await portOpen(port)).toBe(false);
  }
  expect(taggedProcesses(tag)).toEqual([]);
}

describe('hot-switch drill lifecycle', () => {
  test('SIGINT in the startup window leaves no Portal, Server, port or temp dir', async () => {
    for (const delayMs of [25, 40, 55, 70]) {
      const tag = `sigint-${delayMs}-${process.pid}-${Date.now()}`;
      const dirsBefore = tempDirs();
      const child = Bun.spawn({
        cmd: [process.execPath, 'run', DRILL],
        stdin: 'ignore',
        stdout: 'pipe',
        stderr: 'pipe',
        env: { ...process.env, [TAG_NAME]: tag },
      });
      const stdoutPump = readAll(child.stdout);
      const stderrPump = readAll(child.stderr);
      await sleep(delayMs);
      child.kill('SIGINT');
      const code = await waitForExit(child, EXIT_TIMEOUT_MS);
      const stdout = await stdoutPump;
      const stderr = await stderrPump;
      expect(code).toBe(130);
      // The handler owns the exit when the script has started; a signal that
      // beat module loading has nothing to leak, which the checks below cover.
      if (stderr.includes('aborting the drill')) {
        expect(stderr).toContain('interrupted by SIGINT');
      }
      // A Server that was already spawned notices the dead parent in ~500ms.
      await waitFor(
        () => taggedProcesses(tag).length === 0,
        SETTLE_TIMEOUT_MS,
        `tagged processes after SIGINT at ${delayMs}ms`,
      );
      await expectNoLifecycleLeftovers(stdout, dirsBefore, tag);
    }
  }, 90_000);

  test('closing the Portal stdout stops it and its Server', async () => {
    const tag = `epipe-${process.pid}-${Date.now()}`;
    const dataDirectory = mkdtempSync(join(tmpdir(), TEMP_PREFIX));
    const dirsBefore = tempDirs();
    const child = Bun.spawn({
      cmd: [process.execPath, 'run', PORTAL],
      stdin: 'ignore',
      stdout: 'pipe',
      stderr: 'pipe',
      env: {
        ...process.env,
        [TAG_NAME]: tag,
        PORTAL_PORT: '0',
        GAME_SLICE_DATA_DIR: dataDirectory,
        GAME_SLICE_CONTROL: '1',
      },
    });
    const stderrPump = readAll(child.stderr);
    // Drop the read end before the listening line is written.
    await child.stdout.cancel();
    const code = await waitForExit(child, EXIT_TIMEOUT_MS);
    const stderr = await stderrPump;
    try {
      expect(code).not.toBe(0);
      expect(stderr).toContain('stdout closed');
      await waitFor(
        () => taggedProcesses(tag).length === 0,
        SETTLE_TIMEOUT_MS,
        'tagged processes after a closed stdout',
      );
      await expectNoLifecycleLeftovers('', dirsBefore, tag);
    } finally {
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  }, 60_000);
});
