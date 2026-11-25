import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';

import * as wireCodes from './wire-codes.ts';
import {
  AUTH_REQUIRED,
  AuthenticationRequiredError,
  BusinessError,
  FORBIDDEN,
  ForbiddenError,
  FrameworkTimeoutError,
  INTERNAL_ERROR,
  RequestCancelledError,
  UnknownHandlerError,
  WIRE_CODES,
  describeFailure,
  failureLogLevel,
  isFrameworkCode,
} from './wire-codes.ts';

const codeValues: readonly string[] = WIRE_CODES;

/**
 * The named constants as the module exports them. `WIRE_CODES` derives from the
 * same object, so the table cannot drift from it; what can still drift is a new
 * table entry without a named export, which is what the guard below checks.
 */
const exportedCodeValues = new Set<string>();
for (const value of Object.values(wireCodes)) {
  if (typeof value === 'string') {
    exportedCodeValues.add(value);
  }
}

describe('wire code table', () => {
  test('matches the SPEC 7.2 snapshot exactly, in order', () => {
    // The external wire contract (SPEC 7.2, INV-014), written out rather than
    // derived from the table: a deletion, rename or reorder breaks downstream
    // clients, so it must fail here until someone edits this snapshot on
    // purpose. Adding a code appends it here and in SPEC 7.2.
    const snapshot = [
      'FRAMEWORK_TIMEOUT',
      'FRAMEWORK_QUEUE_FULL',
      'FRAMEWORK_SHUTDOWN',
      'FRAMEWORK_REPLAY_EXHAUSTED',
      'TRANSPORT_CLOSED',
      'TRANSPORT_SEND_FAILED',
      'INVALID_ENVELOPE',
      'UNKNOWN_METHOD',
      'INTERNAL_ERROR',
      'AUTH_REQUIRED',
      'FORBIDDEN',
    ];

    expect(WIRE_CODES).toEqual(snapshot);
  });

  test('every code in the table has a named export', () => {
    // A table entry with no constant would be unaddressable from the call sites
    // that emit it.
    expect(codeValues.length).toBeGreaterThan(0);
    for (const code of codeValues) {
      expect({ code, exported: exportedCodeValues.has(code) }).toEqual({
        code,
        exported: true,
      });
    }
  });

  test('no two codes share a value', () => {
    // A second name for an existing string is a silent alias: both would appear
    // distinct in code while downstream sees one code, which INV-014 forbids.
    expect(new Set(codeValues).size).toBe(codeValues.length);
  });

  test('wire-codes does not import dispatcher', () => {
    // The error types used to live in `dispatcher.ts` and be imported here,
    // while dispatcher imported the codes: a cycle that only worked because the
    // classes were used inside function bodies. Guard the direction that keeps
    // the module a leaf.
    const source = readFileSync(
      new URL('./wire-codes.ts', import.meta.url),
      'utf8',
    );
    expect(source).not.toContain('./dispatcher.ts');
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
  test('logs framework failures at error', () => {
    for (const code of codeValues) {
      if (code === AUTH_REQUIRED || code === FORBIDDEN) {
        continue;
      }
      expect(failureLogLevel({ code })).toBe('error');
    }
  });

  test('logs the expected access refusals at warn', () => {
    // They are framework codes, but a refusal is an expected outcome and must
    // not page anyone (SPEC 7.1, 7.2).
    for (const code of [AUTH_REQUIRED, FORBIDDEN]) {
      expect(isFrameworkCode(code)).toBe(true);
      expect(failureLogLevel({ code })).toBe('warn');
    }
  });

  test('logs business codes at warn', () => {
    for (const code of ['PLAYER_NOT_FOUND', 'FRAMEWORK_LOOKALIKE', '']) {
      expect(failureLogLevel({ code })).toBe('warn');
    }
  });
});

describe('describeFailure', () => {
  test('passes an AuthenticationRequiredError code through', () => {
    const error = new AuthenticationRequiredError('look');

    expect(describeFailure(error)).toEqual({
      code: AUTH_REQUIRED,
      message: error.message,
    });
  });

  test('passes a ForbiddenError code through', () => {
    const error = new ForbiddenError('look');

    expect(describeFailure(error)).toEqual({
      code: FORBIDDEN,
      message: error.message,
    });
  });

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
