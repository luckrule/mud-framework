import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf } from '../testing/common.ts';
import type { HandlerAccess } from './dispatcher.ts';
import type { Extension, HandlerRegistrationOptions } from './extension.ts';
import { startServer } from './server-startup.ts';

describe('ExtensionRegistrar registration validation', () => {
  test('rejects invalid HandlerOptions through the registrar path', async () => {
    const cases: readonly {
      label: string;
      options: HandlerRegistrationOptions;
      pattern: RegExp;
    }[] = [
      {
        label: 'access',
        options: { access: 'admin' as HandlerAccess },
        pattern: /access/,
      },
      {
        label: 'mayChangeSession',
        options: { mayChangeSession: 'yes' as unknown as boolean },
        pattern: /mayChangeSession/,
      },
    ];

    for (const testCase of cases) {
      let captured: unknown;
      const extension: Extension = {
        name: `bad-${testCase.label}`,
        register: (registrar) => {
          try {
            registrar.register('bad', async () => null, testCase.options);
          } catch (error) {
            captured = error;
            throw error;
          }
        },
      };
      const logger = createJsonLogger({ sink: () => undefined });

      const error = await rejectionOf(
        startServer({
          options: {
            createBackend: () => new MemoryStorageBackend(),
            extensions: [extension],
            timeoutMs: 1000,
            logger,
          },
          logger,
        }),
      );

      expect(captured).toBeInstanceOf(TypeError);
      expect((captured as Error).message).toMatch(testCase.pattern);
      expect(error.message).toMatch(testCase.pattern);
    }
  });
});
