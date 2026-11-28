import type { GameEvent } from '../src/client-lib.ts';
import { V2_SAY_PREFIX } from '../src/game.ts';
import type { GameVersion } from '../src/game.ts';
import type { Run } from './types.ts';

/** Small shared utilities, wire-result validators and the run's failure sink. */

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export function write(line: string): void {
  process.stdout.write(`${line}\n`);
}

export function writeError(line: string): void {
  process.stderr.write(`${line}\n`);
}

export function sleep(ms: number): Promise<void> {
  return Bun.sleep(ms);
}

export function nowNs(): number {
  return Bun.nanoseconds();
}

export function asMs(ns: number): number {
  return ns / 1e6;
}

export function formatMs(ms: number): string {
  return ms.toFixed(1);
}

export async function withDeadline<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

export function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !(isRecord(error) && error.code === 'ESRCH');
  }
}

export function killQuietly(pid: number): void {
  try {
    process.kill(pid, 'SIGKILL');
  } catch {
    // Already gone between the liveness check and the kill.
  }
}

/** Records a hard failure: printed immediately and counted in the result. */
export function fail(run: Run, message: string): void {
  run.failures.push(message);
  writeError(`FAIL ${message}`);
}

export function check(run: Run, condition: boolean, message: string): void {
  if (!condition) {
    fail(run, message);
  }
}

/** Serialises one client's commands: a retry must never overlap its original. */
export class SerialQueue {
  private tail: Promise<void> = Promise.resolve();

  run<T>(task: () => Promise<T>): Promise<T> {
    const result = this.tail.then(task, task);
    this.tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  /** Resolves once nothing is in flight; used to align the window say. */
  idle(): Promise<void> {
    return this.tail;
  }
}

export function expectedSayText(version: GameVersion, marker: string): string {
  return version === 'v2' ? `${V2_SAY_PREFIX}${marker}` : marker;
}

// ---------------------------------------------------------------- wire views

export interface RoomView {
  readonly id: string;
  readonly you: string;
}

export interface LoginView {
  readonly sessionId: string;
  readonly room: string;
}

export interface InventoryView {
  readonly itemIds: readonly string[];
}

export function readRoom(result: unknown): RoomView {
  const room = isRecord(result) ? result.room : undefined;
  if (!isRecord(room)) {
    throw new Error('result has no room object');
  }
  const id = room.id;
  const you = room.you;
  if (typeof id !== 'string' || id.length === 0) {
    throw new Error('room.id is not a non-empty string');
  }
  if (typeof you !== 'string' || you.length === 0) {
    throw new Error('room.you is not a non-empty string');
  }
  return { id, you };
}

export function readLogin(result: unknown): LoginView {
  if (!isRecord(result)) {
    throw new Error('login result is not an object');
  }
  const sessionId = result.sessionId;
  if (typeof sessionId !== 'string' || sessionId.length === 0) {
    throw new Error('login result has no sessionId');
  }
  return { sessionId, room: readRoom(result).id };
}

export function readInventory(result: unknown): InventoryView {
  const items = isRecord(result) ? result.items : undefined;
  if (!Array.isArray(items)) {
    throw new Error('inventory result has no items array');
  }
  const list: readonly unknown[] = items;
  const itemIds = list.map((item, index) => {
    const id = isRecord(item) ? item.id : undefined;
    if (typeof id !== 'string' || id.length === 0) {
      throw new Error(`inventory item ${index} has no id`);
    }
    return id;
  });
  return { itemIds };
}

export function sayTextOf(event: GameEvent): string {
  const payload = event.payload;
  const text = isRecord(payload) ? payload.text : undefined;
  if (typeof text !== 'string') {
    throw new Error('room.say payload has no text');
  }
  return text;
}

export function fromOf(event: GameEvent): string | undefined {
  const payload = event.payload;
  const from = isRecord(payload) ? payload.from : undefined;
  return typeof from === 'string' ? from : undefined;
}

export function readPid(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isInteger(value) && value > 0
    ? value
    : undefined;
}
