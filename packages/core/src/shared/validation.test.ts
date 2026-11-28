import { describe, expect, test } from 'bun:test';

import {
  CLIENT_REQUEST_ID_EMPTY_MESSAGE,
  MAX_TIMER_DELAY_MS,
  assertClientRequestId,
  assertPositiveInteger,
  assertPositiveSafeInteger,
  assertTimerDelay,
  clampTimerDelay,
  describeValue,
  isRecord,
  isValidClientRequestId,
  messageOf,
  toError,
} from './validation.ts';

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

describe('toError', () => {
  test('returns the same Error instance', () => {
    const error = new Error('boom');
    expect(toError(error)).toBe(error);
  });

  test('wraps a non-Error with the messageOf wording', () => {
    const wrapped = toError('plain');
    expect(wrapped).toBeInstanceOf(Error);
    expect(wrapped.message).toBe(messageOf('plain'));
    expect(wrapped.message).toBe('plain');
  });
});

describe('clampTimerDelay', () => {
  test.each([
    ['a delay below the ceiling is unchanged', 5000, 5000],
    ['the ceiling itself is unchanged', MAX_TIMER_DELAY_MS, MAX_TIMER_DELAY_MS],
    [
      'a delay above the ceiling is capped',
      MAX_TIMER_DELAY_MS + 1,
      MAX_TIMER_DELAY_MS,
    ],
  ] as const)('%s', (_name, value, expected) => {
    expect(clampTimerDelay(value)).toBe(expected);
  });
});

type Invalid = readonly [unknown, typeof RangeError | typeof TypeError];

const POSITIVE_INTEGER_INVALID: readonly Invalid[] = [
  [0, RangeError],
  [-1, RangeError],
  [1.5, RangeError],
  [Number.NaN, RangeError],
  [Number.POSITIVE_INFINITY, RangeError],
  [Number.NEGATIVE_INFINITY, RangeError],
  ['1', TypeError],
  [null, TypeError],
  [undefined, TypeError],
  [{}, TypeError],
];

describe('assertPositiveInteger', () => {
  test('accepts a whole number of at least 1', () => {
    expect(() => assertPositiveInteger(1, 'count')).not.toThrow();
    expect(() =>
      assertPositiveInteger(Number.MAX_SAFE_INTEGER, 'count'),
    ).not.toThrow();
  });

  test('accepts unsafe integers, unlike the safe-integer guard', () => {
    // `Number.isInteger` accepts these; `Number.isSafeInteger` does not, which
    // is why counts that feed arithmetic use the safe guard instead.
    expect(() => assertPositiveInteger(2 ** 53, 'count')).not.toThrow();
    expect(() => assertPositiveInteger(1e21, 'count')).not.toThrow();
    expect(() => assertPositiveSafeInteger(2 ** 53, 'count')).toThrow(
      RangeError,
    );
  });

  test.each(POSITIVE_INTEGER_INVALID)('rejects %p as %p', (value, Expected) => {
    expect(() => assertPositiveInteger(value, 'count')).toThrow(Expected);
    expect(() => assertPositiveInteger(value, 'count')).toThrow(/count/);
  });

  test('names the received scalar in the rejection', () => {
    expect(() => assertPositiveInteger(1.5, 'count')).toThrow(
      'count must be a positive integer, got 1.5',
    );
    expect(() => assertPositiveInteger('x', 'count')).toThrow(
      'count must be a positive integer, got string "x"',
    );
  });
});

// `Number.isSafeInteger` rejects integral values past 2^53-1, which
// `Number.isInteger` would accept; the cap guards arithmetic on the value.
const POSITIVE_SAFE_INTEGER_INVALID: readonly Invalid[] = [
  [0, RangeError],
  [-1, RangeError],
  [1.5, RangeError],
  [Number.MAX_SAFE_INTEGER + 1, RangeError],
  [Number.NaN, RangeError],
  [Number.POSITIVE_INFINITY, RangeError],
  ['1', TypeError],
  [null, TypeError],
  [undefined, TypeError],
];

describe('assertPositiveSafeInteger', () => {
  test('accepts a safe integer of at least 1', () => {
    expect(() => assertPositiveSafeInteger(1, 'limit')).not.toThrow();
    expect(() =>
      assertPositiveSafeInteger(Number.MAX_SAFE_INTEGER, 'limit'),
    ).not.toThrow();
  });

  test.each(POSITIVE_SAFE_INTEGER_INVALID)(
    'rejects %p as %p',
    (value, Expected) => {
      expect(() => assertPositiveSafeInteger(value, 'limit')).toThrow(Expected);
      expect(() => assertPositiveSafeInteger(value, 'limit')).toThrow(
        /limit must be a safe integer of at least 1/,
      );
    },
  );

  test('accepts a value equal to the named ceiling', () => {
    const ceiling = {
      max: MAX_TIMER_DELAY_MS,
      label: `${MAX_TIMER_DELAY_MS} (2^31-1)`,
    };
    expect(() =>
      assertPositiveSafeInteger(MAX_TIMER_DELAY_MS, 'cap', ceiling),
    ).not.toThrow();
  });

  test('rejects a value above the named ceiling and prints the label', () => {
    const ceiling = {
      max: MAX_TIMER_DELAY_MS,
      label: `${MAX_TIMER_DELAY_MS} (2^31-1)`,
    };
    expect(() =>
      assertPositiveSafeInteger(MAX_TIMER_DELAY_MS + 1, 'cap', ceiling),
    ).toThrow(RangeError);
    expect(() =>
      assertPositiveSafeInteger(MAX_TIMER_DELAY_MS + 1, 'cap', ceiling),
    ).toThrow(
      `cap must be a safe integer between 1 and ${MAX_TIMER_DELAY_MS} (2^31-1), got ${MAX_TIMER_DELAY_MS + 1}`,
    );
  });

  test('rejects a non-number above the named ceiling as a TypeError', () => {
    const ceiling = { max: 10, label: '10 (ten)' };
    expect(() => assertPositiveSafeInteger('11', 'cap', ceiling)).toThrow(
      'cap must be a safe integer between 1 and 10 (ten), got string "11"',
    );
  });
});

const TIMER_DELAY_INVALID: readonly Invalid[] = [
  [0, RangeError],
  [0.5, RangeError],
  [-1, RangeError],
  [Number.NaN, RangeError],
  [Number.POSITIVE_INFINITY, RangeError],
  [Number.NEGATIVE_INFINITY, RangeError],
  [MAX_TIMER_DELAY_MS + 1, RangeError],
  ['100', TypeError],
  [null, TypeError],
  [undefined, TypeError],
];

describe('assertTimerDelay', () => {
  test('accepts 1, a fractional delay and the ceiling', () => {
    // Fractional delays are legal here; `assertExecutorTimerMs` adds the whole
    // millisecond requirement for the options that need it.
    expect(() => assertTimerDelay(1, 'delay')).not.toThrow();
    expect(() => assertTimerDelay(1.5, 'delay')).not.toThrow();
    expect(() => assertTimerDelay(MAX_TIMER_DELAY_MS, 'delay')).not.toThrow();
  });

  test.each(TIMER_DELAY_INVALID)('rejects %p as %p', (value, Expected) => {
    expect(() => assertTimerDelay(value, 'delay')).toThrow(Expected);
    expect(() => assertTimerDelay(value, 'delay')).toThrow(/delay/);
  });

  test('states the timer ceiling separately from the lower bound', () => {
    expect(() => assertTimerDelay(MAX_TIMER_DELAY_MS + 1, 'delay')).toThrow(
      `delay must not exceed ${MAX_TIMER_DELAY_MS}ms (the largest setTimeout delay), got ${MAX_TIMER_DELAY_MS + 1}`,
    );
  });

  test('describes a non-number without serializing it', () => {
    expect(() => assertTimerDelay('100', 'delay')).toThrow(
      'delay must be a finite number of at least 1, got string "100"',
    );
  });
});

describe('isValidClientRequestId', () => {
  test.each([
    ['undefined means none', undefined, true],
    ['a non-empty id is valid', 'client-1', true],
    ['an empty id is invalid', '', false],
  ] as const)('%s', (_name, value, expected) => {
    expect(isValidClientRequestId(value)).toBe(expected);
  });
});

describe('assertClientRequestId', () => {
  test('accepts an absent or non-empty id', () => {
    expect(() => assertClientRequestId(undefined)).not.toThrow();
    expect(() => assertClientRequestId('client-1')).not.toThrow();
  });

  test('throws a TypeError with the shared message for an empty id', () => {
    expect(() => assertClientRequestId('')).toThrow(TypeError);
    expect(() => assertClientRequestId('')).toThrow(
      CLIENT_REQUEST_ID_EMPTY_MESSAGE,
    );
  });
});
