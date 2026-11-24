import { describe, expect, test } from 'bun:test';

import {
  FrameworkTimeoutError,
  RequestCancelledError,
  UnknownHandlerError,
} from './dispatcher.ts';
import * as wireCodes from './wire-codes.ts';
import {
  BusinessError,
  INTERNAL_ERROR,
  WIRE_CODES,
  describeFailure,
  failureLogLevel,
  isFrameworkCode,
} from './wire-codes.ts';

/**
 * The code constants as the module actually exports them, discovered rather
 * than listed: a new `export const SOME_CODE = '...'` shows up here without
 * this file being edited, which is the point of the guards below. Only the
 * code strings are exported from `wire-codes.ts`, so a string export is a code.
 */
const moduleExports: Record<string, unknown> = wireCodes;
const exportedCodes: [string, string][] = Object.entries(moduleExports).filter(
  (entry): entry is [string, string] => typeof entry[1] === 'string',
);
const codeValues: readonly string[] = WIRE_CODES;

describe('wire code table', () => {
  test('every exported code constant is in the summary array', () => {
    // Guards the one edit a new code needs: adding the constant without adding
    // it to `WIRE_CODES` would otherwise leave `isFrameworkCode` unaware.
    expect(exportedCodes.length).toBeGreaterThan(0);
    for (const [name, value] of exportedCodes) {
      expect({ name, listed: codeValues.includes(value) }).toEqual({
        name,
        listed: true,
      });
    }
  });

  test('the summary array holds exactly the exported constants', () => {
    // The other direction: a code listed in `WIRE_CODES` with no constant to
    // name it would be unaddressable from the call sites that emit it.
    expect(codeValues.length).toBe(exportedCodes.length);
  });

  test('no two code constants share a value', () => {
    // A second name for an existing string is a silent alias: both would appear
    // distinct in code while downstream sees one code, which INV-014 forbids.
    const values = exportedCodes.map(([, value]) => value);
    expect(new Set(values).size).toBe(values.length);
  });
});

describe('isFrameworkCode', () => {
  test('returns true for every code in the table', () => {
    for (const code of codeValues) {
      expect(isFrameworkCode(code)).toBe(true);
    }
  });

  test('returns false for business codes, lookalikes and the empty code', () => {
    // A prefix rule would classify `FRAMEWORK_LOOKALIKE` as framework and
    // silently downgrade a real failure to a warning; the list is the judgement.
    for (const code of ['PLAYER_NOT_FOUND', 'FRAMEWORK_LOOKALIKE', '']) {
      expect(isFrameworkCode(code)).toBe(false);
    }
  });
});

describe('failureLogLevel', () => {
  test('logs framework codes at error', () => {
    for (const code of codeValues) {
      expect(failureLogLevel({ code })).toBe('error');
    }
  });

  test('logs business codes at warn', () => {
    for (const code of ['PLAYER_NOT_FOUND', 'FRAMEWORK_LOOKALIKE', '']) {
      expect(failureLogLevel({ code })).toBe('warn');
    }
  });
});

describe('describeFailure', () => {
  test('passes a FrameworkTimeoutError code through', () => {
    const error = new FrameworkTimeoutError('req-1', 15);

    expect(describeFailure(error)).toEqual({
      code: 'FRAMEWORK_TIMEOUT',
      message: error.message,
    });
  });

  test('passes an UnknownHandlerError code through', () => {
    const error = new UnknownHandlerError('missing');

    expect(describeFailure(error)).toEqual({
      code: 'UNKNOWN_METHOD',
      message: error.message,
    });
  });

  test('passes a RequestCancelledError code through', () => {
    const error = new RequestCancelledError('req-1', 'TRANSPORT_CLOSED');

    expect(describeFailure(error)).toEqual({
      code: 'TRANSPORT_CLOSED',
      message: error.message,
    });
  });

  test('passes a non-empty BusinessError code through', () => {
    expect(
      describeFailure(new BusinessError('PLAYER_NOT_FOUND', 'gone')),
    ).toEqual({ code: 'PLAYER_NOT_FOUND', message: 'gone' });
  });

  test('degrades a BusinessError with an empty code', () => {
    expect(describeFailure(new BusinessError('', 'unclassifiable'))).toEqual({
      code: INTERNAL_ERROR,
      message: 'unclassifiable',
    });
  });

  test('degrades an ordinary Error, including a duck-typed code', () => {
    // A driver code such as `ENOENT` must not become part of the protocol.
    const error = Object.assign(new Error('no such file'), { code: 'ENOENT' });

    expect(describeFailure(error)).toEqual({
      code: INTERNAL_ERROR,
      message: 'no such file',
    });
  });

  test('degrades a non-Error value', () => {
    expect(describeFailure('boom')).toEqual({
      code: INTERNAL_ERROR,
      message: 'boom',
    });
  });
});
