import { describe, expect, test } from 'bun:test';

import { describeValue, isRecord, messageOf } from './guards.ts';

describe('isRecord', () => {
  test('an empty object is a record', () => {
    expect(isRecord({})).toBe(true);
  });

  test('an object with own fields is a record', () => {
    expect(isRecord({ a: 1 })).toBe(true);
  });

  test('an array is not a record', () => {
    expect(isRecord([])).toBe(false);
    expect(isRecord([1, 2])).toBe(false);
  });

  test('null is not a record', () => {
    expect(isRecord(null)).toBe(false);
  });

  test('a string is not a record', () => {
    expect(isRecord('{}')).toBe(false);
  });

  test('a number is not a record', () => {
    expect(isRecord(42)).toBe(false);
  });

  test('a boolean is not a record', () => {
    expect(isRecord(true)).toBe(false);
  });

  test('undefined is not a record', () => {
    expect(isRecord(undefined)).toBe(false);
  });

  // `typeof` reports 'object' for class instances, and they can be read like
  // one, so a non-plain prototype must not disqualify a value.
  test('an object with a non-Object prototype is a record', () => {
    class Message {
      readonly kind = 'hello';
    }
    expect(isRecord(new Message())).toBe(true);
  });
});

describe('describeValue', () => {
  test('null is named as null', () => {
    expect(describeValue(null)).toBe('null');
  });

  test('an array is described as an array, not its contents', () => {
    expect(describeValue([])).toBe('array');
    expect(describeValue([1, 2, 3])).toBe('array');
  });

  test('a string is quoted so its boundaries are visible', () => {
    expect(describeValue('hello')).toBe('string "hello"');
  });

  test('a string with quotes and newlines is escaped', () => {
    expect(describeValue('a"b\nc')).toBe('string "a\\"b\\nc"');
  });

  test('a number keeps its text form', () => {
    expect(describeValue(42)).toBe('number 42');
    expect(describeValue(1.5)).toBe('number 1.5');
  });

  test('a boolean spells out its value', () => {
    expect(describeValue(true)).toBe('boolean true');
    expect(describeValue(false)).toBe('boolean false');
  });

  test('undefined is named as undefined', () => {
    expect(describeValue(undefined)).toBe('undefined');
  });

  test('a function is described by its type only', () => {
    expect(describeValue(() => undefined)).toBe('function');
  });

  test('a symbol is described by its type only', () => {
    expect(describeValue(Symbol('x'))).toBe('symbol');
  });

  test('an object is described by its type only, never serialized', () => {
    expect(describeValue({ secret: 'client data' })).toBe('object');
  });
});

describe('messageOf', () => {
  test('an Error yields its message', () => {
    expect(messageOf(new Error('boom'))).toBe('boom');
  });

  // An empty message is a real outcome and must not be replaced by the type.
  test('an Error with an empty message yields an empty string', () => {
    expect(messageOf(new Error(''))).toBe('');
  });

  test('a string is returned unchanged', () => {
    expect(messageOf('plain')).toBe('plain');
  });

  test('null is stringified', () => {
    expect(messageOf(null)).toBe('null');
  });

  test('undefined is stringified', () => {
    expect(messageOf(undefined)).toBe('undefined');
  });

  test('a number is stringified', () => {
    expect(messageOf(42)).toBe('42');
  });

  // Pinned: a thrown plain object loses its fields, because `String` is what
  // the non-Error branch has always used.
  test('a plain object falls back to Object.prototype.toString', () => {
    expect(messageOf({ code: 'X' })).toBe('[object Object]');
  });
});
