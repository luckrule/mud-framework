import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

import { SqliteBackend } from 'mud-framework-storage-sqlite';
import {
  isProcessAlive,
  isRecord,
  LineReader,
  sleep,
  tempDatabase,
  withDeadline,
} from 'mud-framework-test-support';

import {
  GameClient,
  GameClientError,
} from '../../examples/game-slice/src/client-lib.ts';
import {
  clearPlayerSessionIfHolder,
  findPlayerByName,
  initializeWorld,
  upsertPlayerLogin,
} from '../../examples/game-slice/src/world.ts';
import type {
  ItemView,
  RoomSnapshot,
} from '../../examples/game-slice/src/world.ts';

/**
 * End-to-end slice over the real two-process path: this test spawns
 * `examples/game-slice/src/portal.ts`, which spawns the real Server entry. The
 * data directory is temporary and the port is OS-assigned (`PORTAL_PORT=0`);
 * the Portal prints the address and this test parses it.
 */
const PORTAL_ENTRY = join(
  import.meta.dir,
  '..',
  '..',
  'examples',
  'game-slice',
  'src',
  'portal.ts',
);
const READY_TIMEOUT_MS = 20_000;
const REQUEST_TIMEOUT_MS = 5000;
const EVENT_TIMEOUT_MS = 5000;
const PORTAL_EXIT_TIMEOUT_MS = 20_000;
const FORCE_EXIT_TIMEOUT_MS = 5000;
const SERVER_EXIT_TIMEOUT_MS = 5000;
const SERVER_PID_TIMEOUT_MS = 5000;
const POLL_INTERVAL_MS = 50;
const STDERR_TAIL_LINES = 20;

interface LoginResult {
  readonly sessionId: string;
  readonly room: RoomSnapshot;
}

interface MoveResult {
  readonly moved: boolean;
  readonly replayed?: boolean;
  readonly room: RoomSnapshot;
}

interface TakeResult {
  readonly item: ItemView;
  readonly alreadyOwned: boolean;
}

interface InventoryResult {
  readonly items: ItemView[];
}

interface RunningPortal {
  readonly process: Bun.Subprocess<'ignore', 'pipe', 'pipe'>;
  readonly url: string;
  readonly lines: string[];
  readonly serverPids: number[];
  readonly stderrLines: string[];
  /** Idempotent: SIGTERM, then SIGKILL if needed, then reap the Server pids. */
  stop(): Promise<number | null | undefined>;
}

function listeningUrl(line: string): string | undefined {
  const match = /^portal: listening on (ws:\/\/\S+)$/.exec(line);
  return match?.[1];
}

function serverPidFromLine(line: string): number | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (!isRecord(parsed)) {
    return undefined;
  }
  if (parsed.msg !== 'server process ready' && parsed.msg !== 'server ready') {
    return undefined;
  }
  const pid = parsed.pid;
  return typeof pid === 'number' && Number.isInteger(pid) && pid > 0
    ? pid
    : undefined;
}

async function waitForExit(
  child: Bun.Subprocess<'ignore', 'pipe', 'pipe'>,
  timeoutMs: number,
): Promise<number | undefined> {
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

async function waitGone(pid: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isProcessAlive(pid)) {
      return true;
    }
    await sleep(POLL_INTERVAL_MS);
  }
  return !isProcessAlive(pid);
}

async function startPortal(dataDirectory: string): Promise<RunningPortal> {
  const child = Bun.spawn({
    cmd: [process.execPath, 'run', PORTAL_ENTRY],
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
    env: {
      ...process.env,
      PORTAL_PORT: '0',
      GAME_SLICE_DATA_DIR: dataDirectory,
    },
  });

  const lines: string[] = [];
  const serverPids: number[] = [];
  const stderrLines: string[] = [];
  const stdoutReader = new LineReader(child.stdout);
  const stderrReader = new LineReader(child.stderr);
  const stderrPump = (async () => {
    let line = await stderrReader.nextLine();
    while (line !== undefined) {
      stderrLines.push(line);
      line = await stderrReader.nextLine();
    }
  })().catch(() => undefined);

  let url: string | undefined;
  let stdoutPump: Promise<void> | undefined;
  try {
    while (url === undefined) {
      const line = await withDeadline(
        stdoutReader.nextLine(),
        READY_TIMEOUT_MS,
        'portal listening line',
      );
      if (line === undefined) {
        throw new Error(
          `portal stdout closed before the listening line; stderr: ${stderrTail(stderrLines)}`,
        );
      }
      lines.push(line);
      const pid = serverPidFromLine(line);
      if (pid !== undefined) {
        serverPids.push(pid);
      }
      url = listeningUrl(line);
    }

    stdoutPump = (async () => {
      let line = await stdoutReader.nextLine();
      while (line !== undefined) {
        lines.push(line);
        const pid = serverPidFromLine(line);
        if (pid !== undefined) {
          serverPids.push(pid);
        }
        line = await stdoutReader.nextLine();
      }
    })().catch(() => undefined);

    // The Server logs its pid just after sending `ready`; the listening line
    // can win the race by a scheduling hop. Waiting here means cleanup always
    // knows which pid to verify, even if the test body fails immediately.
    const deadline = Date.now() + SERVER_PID_TIMEOUT_MS;
    while (serverPids.length === 0) {
      if (Date.now() >= deadline) {
        throw new Error(
          `no "server process ready" line within ${SERVER_PID_TIMEOUT_MS}ms`,
        );
      }
      await sleep(POLL_INTERVAL_MS);
    }
  } catch (error) {
    child.kill('SIGKILL');
    await waitForExit(child, FORCE_EXIT_TIMEOUT_MS);
    for (const pid of serverPids) {
      if (isProcessAlive(pid)) {
        try {
          process.kill(pid, 'SIGKILL');
        } catch {
          // Already gone between the check and the kill.
        }
      }
    }
    await Promise.all(
      serverPids.map((pid) => waitGone(pid, SERVER_EXIT_TIMEOUT_MS)),
    );
    await Promise.race([
      Promise.allSettled([stdoutPump, stderrPump]),
      sleep(2000),
    ]);
    throw error;
  }

  let stopped: Promise<number | null | undefined> | undefined;
  const stop = (): Promise<number | null | undefined> => {
    stopped ??= (async () => {
      child.kill('SIGTERM');
      const code = await waitForExit(child, PORTAL_EXIT_TIMEOUT_MS);
      if (code === undefined) {
        child.kill('SIGKILL');
        await waitForExit(child, FORCE_EXIT_TIMEOUT_MS);
      }
      // Reap before waiting for the pipes: the Server inherits this test's
      // stdout/stderr, so a surviving child would keep them open.
      for (const pid of serverPids) {
        if (isProcessAlive(pid)) {
          try {
            process.kill(pid, 'SIGKILL');
          } catch {
            // Already gone between the check and the kill.
          }
        }
      }
      for (const pid of serverPids) {
        await waitGone(pid, SERVER_EXIT_TIMEOUT_MS);
      }
      await Promise.race([
        Promise.allSettled([stdoutPump, stderrPump]),
        sleep(2000),
      ]);
      return code ?? child.exitCode;
    })();
    return stopped;
  };

  return {
    process: child,
    url,
    lines,
    serverPids,
    stderrLines,
    stop,
  };
}

function stderrTail(lines: readonly string[]): string {
  return lines.slice(-STDERR_TAIL_LINES).join('\n');
}

describe('game slice over a real Portal and Server', () => {
  test('delivers room events and restores room and inventory after takeover', async () => {
    const database = tempDatabase({ prefix: 'mud-game-slice-' });
    const clients: GameClient[] = [];
    let portal: RunningPortal | undefined;
    try {
      portal = await startPortal(database.directory);
      expect(portal.serverPids.length).toBeGreaterThan(0);

      const alice = await GameClient.connect({
        url: portal.url,
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      clients.push(alice);
      const bob = await GameClient.connect({
        url: portal.url,
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      clients.push(bob);

      const aliceLogin = await alice.call<LoginResult>('game.login', {
        name: 'alice',
      });
      expect(aliceLogin.sessionId).toBe('player:alice');
      expect(aliceLogin.room).toMatchObject({ id: 'square', you: 'alice' });

      const bobLogin = await bob.call<LoginResult>('game.login', {
        name: 'bob',
      });
      expect(bobLogin.room.id).toBe('square');

      // Same room: Alice's say reaches Bob as an event, not a response.
      const bobSawSay = bob.nextEvent(
        (event) => event.event === 'room.say',
        EVENT_TIMEOUT_MS,
      );
      const said = await alice.call('game.say', { text: 'hello bob' });
      expect(said).toEqual({ said: true });
      const sayEvent = await withDeadline(
        bobSawSay,
        EVENT_TIMEOUT_MS,
        'room.say event at bob',
      );
      expect(sayEvent.payload).toEqual({ from: 'alice', text: 'hello bob' });
      expect(sayEvent.eventId).toMatch(/:\d+$/);

      // Alice leaves the square: Bob, still there, receives room.left.
      const bobSawLeft = bob.nextEvent(
        (event) => event.event === 'room.left',
        EVENT_TIMEOUT_MS,
      );
      const aliceMoved = await alice.call<MoveResult>('game.move', {
        direction: 'north',
      });
      expect(aliceMoved.moved).toBe(true);
      expect(aliceMoved.room.id).toBe('market');
      const leftEvent = await withDeadline(
        bobSawLeft,
        EVENT_TIMEOUT_MS,
        'room.left event at bob',
      );
      expect(leftEvent.payload).toEqual({
        player: 'alice',
        direction: 'north',
        from: 'square',
        to: 'market',
      });

      // Bob follows her: Alice, already in the market, receives room.entered.
      const aliceSawEnter = alice.nextEvent(
        (event) => event.event === 'room.entered',
        EVENT_TIMEOUT_MS,
      );
      const bobMoved = await bob.call<MoveResult>('game.move', {
        direction: 'north',
      });
      expect(bobMoved.room.id).toBe('market');
      const enteredEvent = await withDeadline(
        aliceSawEnter,
        EVENT_TIMEOUT_MS,
        'room.entered event at alice',
      );
      expect(enteredEvent.payload).toMatchObject({
        player: 'bob',
        from: 'square',
        to: 'market',
      });

      // Persisted state: the bread moves from the market floor to Alice.
      const taken = await alice.call<TakeResult>('game.take', {
        item: 'bread',
      });
      expect(taken.item).toEqual({ id: 'bread', name: 'loaf of bread' });
      expect(taken.alreadyOwned).toBe(false);
      const inventory = await alice.call<InventoryResult>('game.inventory');
      expect(inventory.items.map((item) => item.id)).toContain('bread');

      // Same-name login on a new connection: takeover plus persistence. The
      // new connection sees the room and inventory committed by the old one.
      const aliceAgain = await GameClient.connect({
        url: portal.url,
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      clients.push(aliceAgain);
      const relogin = await aliceAgain.call<LoginResult>('game.login', {
        name: 'alice',
      });
      expect(relogin.sessionId).toBe('player:alice');
      expect(relogin.room.id).toBe('market');
      const inventoryAgain =
        await aliceAgain.call<InventoryResult>('game.inventory');
      expect(inventoryAgain.items.map((item) => item.id)).toContain('bread');

      // The old connection no longer holds the Session (SPEC 3.2): a session
      // Handler is refused before it executes.
      const stale = await alice.call('game.look').then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(stale).toBeInstanceOf(GameClientError);
      if (stale instanceof GameClientError) {
        expect(stale.code).toBe('AUTH_REQUIRED');
      }

      // Its logout is refused by the same gate, and the new holder is intact.
      const staleLogout = await alice.call('game.logout').then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(staleLogout).toBeInstanceOf(GameClientError);
      if (staleLogout instanceof GameClientError) {
        expect(staleLogout.code).toBe('AUTH_REQUIRED');
      }
      const stillHeld = await aliceAgain.call<{ room: RoomSnapshot }>(
        'game.look',
      );
      expect(stillHeld.room.id).toBe('market');

      // The current holder's logout clears the online marker and succeeds.
      const logout = await aliceAgain.call<{
        loggedOut: boolean;
        superseded?: boolean;
      }>('game.logout');
      expect(logout).toEqual({ loggedOut: true });
      const afterLogout = await aliceAgain.call('game.look').then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(afterLogout).toBeInstanceOf(GameClientError);

      const exitCode = await portal.stop();
      expect(exitCode).toBe(0);
      for (const pid of portal.serverPids) {
        expect(await waitGone(pid, SERVER_EXIT_TIMEOUT_MS)).toBe(true);
      }
    } finally {
      await Promise.allSettled(clients.map((client) => client.close()));
      if (portal !== undefined) {
        await portal.stop();
      }
      database.remove();
    }
  }, 60_000);
});

describe('holder guard', () => {
  test('a stale connection cannot clear the new holder session', async () => {
    const database = tempDatabase({ prefix: 'mud-game-slice-holder-' });
    const backend = SqliteBackend.open({ filename: database.path });
    try {
      await initializeWorld(backend);
      // Deterministic version of the login/logout race: the second upsert is
      // the takeover, and the first connection's later cleanup must lose.
      await backend.transaction(async (tx) => {
        await upsertPlayerLogin(
          tx,
          'alice',
          'player:alice',
          'square',
          'connection-old',
        );
        await upsertPlayerLogin(
          tx,
          'alice',
          'player:alice',
          'square',
          'connection-new',
        );

        expect(
          await clearPlayerSessionIfHolder(tx, 'alice', 'connection-old'),
        ).toBe(false);
        const kept = await findPlayerByName(tx, 'alice');
        expect(kept?.session_id).toBe('player:alice');
        expect(kept?.holder_connection_id).toBe('connection-new');

        expect(
          await clearPlayerSessionIfHolder(tx, 'alice', 'connection-new'),
        ).toBe(true);
        const cleared = await findPlayerByName(tx, 'alice');
        expect(cleared?.session_id).toBeNull();
        expect(cleared?.holder_connection_id).toBeNull();
      });
    } finally {
      await backend.close();
      database.remove();
    }
  });
});
