import type { StorageRequirements } from '../storage/requirements.ts';
import type { Dispatcher } from './dispatcher.ts';

/**
 * A packaged unit of business capability (SPEC 2): it registers handlers and
 * declares the storage capabilities it cannot work without.
 */
export interface Extension {
  /** Names the extension in logs; the only stable handle on it. */
  readonly name: string;
  /** Minimum storage capabilities. Omitted means it runs on any backend. */
  readonly requires?: StorageRequirements;
  /**
   * Marks the extension as required for this process to start (SPEC 6.2 step
   * 2). A required extension whose requirements are unmet refuses the whole
   * startup instead of being skipped.
   */
  readonly required?: boolean;
  register(dispatcher: Dispatcher): void | Promise<void>;
}
