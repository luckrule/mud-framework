import { describe, expect, test } from 'bun:test';

import { throwingLogger } from '../test-support/common.ts';
import { createJsonLogger } from './json-logger.ts';
import { safeLogger } from './safe-logger.ts';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseLine(line: string): Record<string, unknown> {
  const parsed: unknown = JSON.parse(line);
  if (!isRecord(parsed)) {
    throw new Error(`line is not a JSON object: ${line}`);
  }
  return parsed;
}

describe('safeLogger', () => {
  test('swallows a throw from every level', () => {
    const logger = safeLogger(throwingLogger());

    expect(() => {
      logger.debug({}, 'debug');
    }).not.toThrow();
    expect(() => {
      logger.info({}, 'info');
    }).not.toThrow();
    expect(() => {
      logger.warn({}, 'warn');
    }).not.toThrow();
    expect(() => {
      logger.error({}, 'error');
    }).not.toThrow();
  });

  test('returns a safe child recursively', () => {
    const logger = safeLogger(throwingLogger());

    const child = logger.child({ requestId: 'r1' });
    expect(() => {
      child.warn({}, 'child');
    }).not.toThrow();

    const grandchild = child.child({ method: 'echo' });
    expect(() => {
      grandchild.error({}, 'grandchild');
    }).not.toThrow();
  });

  test('keeps normal calls and child bindings intact', () => {
    const lines: string[] = [];
    const logger = safeLogger(
      createJsonLogger({
        level: 'debug',
        bindings: { component: 'test' },
        sink: (line) => {
          lines.push(line);
        },
      }),
    );

    logger.info({ a: 1 }, 'parent');
    const child = logger.child({ requestId: 'r1' });
    child.info({ b: 2 }, 'child');

    expect(lines.map(parseLine)).toEqual([
      {
        level: 'info',
        time: expect.any(String),
        msg: 'parent',
        component: 'test',
        a: 1,
      },
      {
        level: 'info',
        time: expect.any(String),
        msg: 'child',
        component: 'test',
        requestId: 'r1',
        b: 2,
      },
    ]);
  });

  test('a throwing child() falls back to the parent wrapper', () => {
    const logger = safeLogger(throwingLogger());

    const child = logger.child({ a: 1 });
    expect(child).toBe(logger);
    expect(() => {
      child.debug({}, 'still safe');
    }).not.toThrow();
  });

  test('does not stack wrappers', () => {
    const logger = safeLogger(throwingLogger());

    expect(safeLogger(logger)).toBe(logger);
  });
});
