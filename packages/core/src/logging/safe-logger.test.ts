import { describe, expect, test } from 'bun:test';

import {
  parseJsonLine as parseLine,
  throwingLogger,
} from 'mud-framework-test-support';

import { createJsonLogger } from './json-logger.ts';
import { safeLogger } from './safe-logger.ts';
import type { LogLevel } from './types.ts';

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

    expect(lines.map((line) => parseLine(line))).toEqual([
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

  test('forwards setLevel to the wrapped logger', () => {
    const lines: string[] = [];
    const logger = safeLogger(
      createJsonLogger({
        level: 'warn',
        sink: (line) => {
          lines.push(line);
        },
      }),
    );

    logger.setLevel('debug');
    logger.debug({ a: 1 }, 'enabled');

    expect(lines.map((line) => parseLine(line))).toEqual([
      { level: 'debug', time: expect.any(String), msg: 'enabled', a: 1 },
    ]);
  });

  test('propagates an invalid level without changing the wrapped logger', () => {
    const lines: string[] = [];
    const logger = safeLogger(
      createJsonLogger({
        level: 'warn',
        sink: (line) => {
          lines.push(line);
        },
      }),
    );

    expect(() => {
      logger.setLevel('trace' as unknown as LogLevel);
    }).toThrow(TypeError);
    expect(() => {
      logger.setLevel('trace' as unknown as LogLevel);
    }).toThrow(/trace/);

    logger.info({}, 'filtered');
    logger.warn({}, 'kept');

    expect(lines.map((line) => parseLine(line).level)).toEqual(['warn']);
  });

  test('does not swallow a throwing setLevel', () => {
    const logger = safeLogger(throwingLogger());

    expect(() => {
      logger.setLevel('debug');
    }).toThrow('logger exploded');
  });
});
