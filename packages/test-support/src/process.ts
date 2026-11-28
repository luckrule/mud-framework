import { isRecord } from './json.ts';

/**
 * `process.kill(pid, 0)` is the only portable liveness probe: it throws ESRCH
 * for a reaped process. Anything else (EPERM included) means it still exists.
 */
export function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const code = isRecord(error) ? error.code : undefined;
    return code !== 'ESRCH';
  }
}
