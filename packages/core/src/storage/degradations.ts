import type { StorageCapabilities } from './types.ts';

export interface Degradation {
  /** The capability that is weaker than its strongest level (SPEC 4.2). */
  readonly capability: string;
  /** What the backend declares. */
  readonly declared: string | boolean;
  /** What the framework therefore does not provide. */
  readonly consequence: string;
}

/**
 * Lists the capabilities `declared` falls short of, one entry per dimension
 * (SPEC 4.3). Empty means the backend offers every capability at full strength,
 * so nothing is degraded and nothing needs warning about.
 */
export function degradations(declared: StorageCapabilities): Degradation[] {
  const found: Degradation[] = [];
  // Fixed order (transactions, concurrency, snapshotRead, uniqueConstraint) so
  // the startup warnings read the same way for every backend.
  if (declared.transactions === 'limited') {
    found.push({
      capability: 'transactions',
      declared: declared.transactions,
      consequence:
        '跨实体写入不保证原子，需要多实体原子性的 Extension 必须声明需求',
    });
  }
  if (declared.transactions === 'none') {
    found.push({
      capability: 'transactions',
      declared: declared.transactions,
      consequence:
        '无回滚，失败时已写入的部分保留，Extension 必须自行补偿或幂等',
    });
  }
  if (declared.concurrency === 'single') {
    found.push({
      capability: 'concurrency',
      declared: declared.concurrency,
      consequence: '框架层并发调度不转化为存储层并发，长事务会阻塞其他请求',
    });
  }
  if (!declared.snapshotRead) {
    found.push({
      capability: 'snapshotRead',
      declared: declared.snapshotRead,
      consequence: '事务内重复读不保证一致，Extension 自行处理读到变化数据',
    });
  }
  if (!declared.uniqueConstraint) {
    found.push({
      capability: 'uniqueConstraint',
      declared: declared.uniqueConstraint,
      consequence: '不提供"检查再写入"的原子性，唯一性由单写者或业务幂等键保证',
    });
  }
  return found;
}
