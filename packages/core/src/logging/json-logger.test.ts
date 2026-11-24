import { describe, expect, test } from 'bun:test';

import { parseJsonLine as parseLine } from 'mud-framework-test-support';

import { JsonLogger, createJsonLogger } from './json-logger.ts';
import type { LogFields } from './types.ts';

interface Captured {
  lines: string[];
  logger: JsonLogger;
  parsed: () => Record<string, unknown>[];
}

function capture(
  options: {
    level?: 'debug' | 'info' | 'warn' | 'error';
    bindings?: LogFields;
  } = {},
): Captured {
  const lines: string[] = [];
  const logger = new JsonLogger({
    ...options,
    sink: (line) => {
      lines.push(line);
    },
  });
  return {
    lines,
    logger,
    parsed: () => lines.map((line) => parseLine(line)),
  };
}

describe('JsonLogger', () => {
  test('filters below the configured level', () => {
    const { logger, parsed } = capture({ level: 'warn' });

    logger.debug({ a: 1 }, 'debug');
    logger.info({ a: 2 }, 'info');
    logger.warn({ a: 3 }, 'warn');
    logger.error({ a: 4 }, 'error');

    expect(parsed().map((entry) => entry.level)).toEqual(['warn', 'error']);
  });

  test('defaults to info', () => {
    const { logger, parsed } = capture();

    logger.debug({}, 'hidden');
    logger.info({}, 'shown');

    expect(parsed()).toHaveLength(1);
    expect(parsed()[0]?.msg).toBe('shown');
  });

  test('writes one line of valid JSON per event with level, time and msg', () => {
    const { logger, lines, parsed } = capture();

    logger.info({}, 'hello');

    expect(lines).toHaveLength(1);
    expect(lines[0]).not.toContain('\n');
    const entry = parsed()[0];
    expect(entry).toMatchObject({ level: 'info', msg: 'hello' });
    expect(typeof entry?.time).toBe('string');
    expect(Number.isNaN(Date.parse(String(entry?.time)))).toBe(false);
  });

  test('merges bindings with call fields, call fields winning', () => {
    const { logger, parsed } = capture({ bindings: { a: 1, b: 2 } });

    logger.info({ b: 3, c: 4 }, 'merged');

    expect(parsed()[0]).toMatchObject({ a: 1, b: 3, c: 4 });
  });

  test('child inherits parent bindings and lets its own override them', () => {
    const { logger, parsed } = capture({ bindings: { a: 1, b: 2 } });

    const child = logger.child({ b: 9, c: 3 });
    child.info({}, 'from child');
    logger.info({}, 'from parent');

    expect(parsed()[0]).toMatchObject({ a: 1, b: 9, c: 3 });
    expect(parsed()[1]).toMatchObject({ a: 1, b: 2 });
    expect(parsed()[1]?.c).toBeUndefined();
  });

  test('child keeps the parent level', () => {
    const { logger, parsed } = capture({ level: 'error' });

    logger.child({ a: 1 }).warn({}, 'dropped');

    expect(parsed()).toHaveLength(0);
  });

  test('degrades to a simplified line when fields cannot be serialized', () => {
    const { logger, lines, parsed } = capture();
    const cyclic: LogFields = { name: 'cycle' };
    cyclic.self = cyclic;

    expect(() => {
      logger.info(cyclic, 'cyclic fields');
    }).not.toThrow();

    expect(lines).toHaveLength(1);
    const entry = parsed()[0];
    expect(entry).toMatchObject({ level: 'info', msg: 'cyclic fields' });
    expect(String(entry?.logError)).toMatch(/could not be serialized/);
    expect(entry?.self).toBeUndefined();
  });

  test('degrades on a BigInt field instead of throwing', () => {
    const { logger, parsed } = capture();

    expect(() => {
      logger.info({ big: 1n }, 'bigint field');
    }).not.toThrow();

    expect(String(parsed()[0]?.logError)).toMatch(/could not be serialized/);
  });

  test('createJsonLogger returns a logger using the injected sink', () => {
    const lines: string[] = [];
    const logger = createJsonLogger({ sink: (line) => lines.push(line) });

    logger.warn({ x: 1 }, 'via factory');

    expect(parseLine(lines[0] ?? '')).toMatchObject({ level: 'warn', x: 1 });
  });
});
