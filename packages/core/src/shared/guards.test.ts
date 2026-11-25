import { describe, expect, test } from 'bun:test';

import { describeValue, isRecord, messageOf } from './guards.ts';

// `typeof` reports 'object' for class instances, and they can be read like one,
// so a non-plain prototype must not disqualify a value.
class Message {
  readonly kind = 'hello';
}

const IS_RECORD: readonly [string, unknown, boolean][] = [
  ['an empty object is a record', {}, true],
  ['an object with own fields is a record', { a: 1 }, true],
  ['an object with a non-Object prototype is a record', new Message(), true],
  ['an array is not a record', [], false],
  ['null is not a record', null, false],
  ['a string is not a record', '{}', false],
  ['a number is not a record', 42, false],
  ['a boolean is not a record', true, false],
  ['undefined is not a record', undefined, false],
];

describe('isRecord', () => {
  test.each(IS_RECORD)('%s', (_name, value, expected) => {
    expect(isRecord(value)).toBe(expected);
  });
});

const DESCRIBE_VALUE: readonly [string, unknown, string][] = [
  ['null is named as null', null, 'null'],
  ['an array is described as an array, not its contents', [1, 2, 3], 'array'],
  [
    'a string is quoted so its boundaries are visible',
    'hello',
    'string "hello"',
  ],
  [
    'a string with quotes and newlines is escaped',
    'a"b\nc',
    'string "a\\"b\\nc"',
  ],
  ['a number keeps its text form', 42, 'number 42'],
  ['a fractional number keeps its text form', 1.5, 'number 1.5'],
  ['a boolean spells out its value', true, 'boolean true'],
  ['false spells out its value', false, 'boolean false'],
  ['undefined is named as undefined', undefined, 'undefined'],
  ['a function is described by its type only', () => undefined, 'function'],
  ['a symbol is described by its type only', Symbol('x'), 'symbol'],
  [
    'an object is described by its type only, never serialized',
    { secret: 'client data' },
    'object',
  ],
];

describe('describeValue', () => {
  test.each(DESCRIBE_VALUE)('%s', (_name, value, expected) => {
    expect(describeValue(value)).toBe(expected);
  });
});

// An empty message is a real outcome and must not be replaced by the type.
// A thrown plain object loses its fields because `String` is what the
// non-Error branch has always used.
const MESSAGE_OF: readonly [string, unknown, string][] = [
  ['an Error yields its message', new Error('boom'), 'boom'],
  ['an Error with an empty message yields an empty string', new Error(''), ''],
  ['a string is returned unchanged', 'plain', 'plain'],
  ['null is stringified', null, 'null'],
  ['undefined is stringified', undefined, 'undefined'],
  ['a number is stringified', 42, '42'],
  [
    'a plain object falls back to Object.prototype.toString',
    { code: 'X' },
    '[object Object]',
  ],
];

describe('messageOf', () => {
  test.each(MESSAGE_OF)('%s', (_name, value, expected) => {
    expect(messageOf(value)).toBe(expected);
  });
});
