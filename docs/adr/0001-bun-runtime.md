# ADR-0001: 选择 Bun 作为运行时

## 背景

框架需要 SQLite 访问、HTTP/WebSocket、子进程与 IPC、测试、打包与 TypeScript 直跑。逐项安装 npm 依赖会形成版本矩阵：各组件独立发版，兼容窗口不重合，每个进程启动还要额外加载与初始化。P4 要求业务进程可低成本拉起并与父进程通信，INV-016 要求框架包不含存储后端实现，依赖面必须窄；运行时的启动开销会直接乘到每次业务进程重启上。

## 决定

锁定单一 Bun 版本，三处一致：`.bun-version`、`package.json` 的 `engines`、`@types/bun`；具体版本以 `.bun-version` 为准。

用内置能力替代外部依赖，不引入运行时 npm 依赖：

- `bun:sqlite` 为独立的 SQLite 后端包提供驱动，该后端不在框架包内（ADR-0003）
- `Bun.serve` 承担 HTTP 与 WebSocket upgrade，含原生 topic 订阅
- `Bun.spawn` 承担业务进程拉起与 IPC（ADR-0002）
- `bun test` 承担测试
- `bun build --compile` 承担单文件可执行产物
- TypeScript 直接执行，无编译步骤

`Bun.SQL` 不作为默认路径：它没有能力查询 API、没有事务超时，方言会泄漏给调用方，无法满足 SPEC 4.2 的能力声明要求。

## 后果

P2 承诺的是 Handler 可 await、调度建立在 Promise 上，不是每次存储调用都真异步。`bun:sqlite` 的同步性不违反 P2，但意味着存储阶段没有并行：框架层的跨连接并发调度落到单连接 SQLite 上会退化为串行。这一限制由 SPEC 4.3 的 `concurrency: single` 降级写成显式契约。

获得：零运行时依赖；TS 直跑省掉编译与产物管理；内置组件版本天然一致；子进程启动成本低。

代价（多数无法靠配置消除）：

- `bun:sqlite` 只有同步 API，长查询会阻塞事件循环，拖慢进程内全部连接。
- SQLite 单连接：请求 A `BEGIN IMMEDIATE` 后 await，请求 B 的写入会被并入 A 的事务；A 回滚时 B 的写入一并消失。异步回调里首个 await 处即提交，此后抛错不回滚。
- `worker_threads` 只是部分实现；`AsyncLocalStorage` 不进入 `Bun.serve` 的 handler，请求上下文只能显式传参。
- `Bun.spawn` 的 IPC 仅兼容其他 Bun 进程，跨 Node 需退化为 JSON 序列化。
- Windows 没有 tier 分级声明。

缓解：SQLite 后端按 `concurrency: 'single'` 声明并内部串行化事务（ADR-0003）；需要真异步存储时改用 Postgres 后端。
