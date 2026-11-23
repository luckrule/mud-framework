# ADR-0001: 选择 Bun 作为运行时

## 背景

框架需要 SQLite 访问、HTTP/WebSocket、子进程与 IPC、测试、打包、TypeScript 直跑。逐项装 npm 依赖会带来版本矩阵：驱动、HTTP 框架、WebSocket 库、测试运行器、打包器各自发版，兼容窗口不重合，且每个进程启动都要额外加载与初始化。

P4 要求业务进程可替换、崩溃后重启，运行时必须能低成本拉起子进程并与之通信；INV-016 要求框架包不含任何存储后端实现，框架自身的依赖面必须窄。运行时的启动开销会直接乘到每次业务进程重启上。

版本必须锁定：Bun 演进很快（1.0 发布于 2023-09-08，1.4 于 2026-08-20 由 Zig 改写为 Rust），运行时、类型定义与 engines 声明不一致时，本地与 CI 会跑在不同行为上。

## 决定

锁定 Bun 1.4.5（发布于 2026-11-19），三处一致：`.bun-version`、`engines`、`bun-types@1.4.5`。

用内置能力替代外部依赖，不引入运行时 npm 依赖：

- `bun:sqlite`（SQLite 3.54.1）为独立的 SQLite 后端包提供驱动，该后端不在框架包内（见 ADR-0003）
- `Bun.serve` 承担 HTTP 与 WebSocket upgrade，含原生 topic 订阅
- `Bun.spawn` 承担业务进程拉起与 IPC（见 ADR-0002）
- `bun test` 承担测试（Jest 兼容、`--parallel`、覆盖率）
- `bun build --compile` 承担单文件可执行产物
- TypeScript 直接执行，无编译步骤

`Bun.SQL` 不作为默认路径：它没有能力查询 API、没有事务超时，方言会泄漏给调用方（MySQL 的 DDL 隐式提交、RETURNING 支持差异、占位符差异、三套错误码），无法满足 SPEC 4.2 的能力声明要求。

## 后果

P2 承诺的是 Handler 可 await、调度建立在 Promise 上，不是每次存储调用都真异步。`bun:sqlite` 的同步性不违反 P2，但意味着存储阶段没有并行：框架层的跨连接并发调度落到单连接 SQLite 上会退化为串行。这一限制由 SPEC 4.3 的 `concurrency: single` 降级写成显式契约，而不是藏在实现里。

获得：零依赖运行时；本机 hello-world 启动实测 Bun 5–6ms、Node 20–26ms；TS 直跑省掉编译与产物管理；内置组件版本天然一致。

代价（多数无法靠配置消除）：

- `bun:sqlite` 只有同步 API，无任何 Promise 方法。实测 287ms 的查询期间事件循环 tick 为 0；16.4 秒查询使主线程停顿 16.04 秒，拖慢进程内全部连接。
- SQLite 单连接：请求 A `BEGIN IMMEDIATE` 后 await，请求 B 的写入被并入 A 的事务，A 回滚时 B 的写入一并消失。`db.transaction()` 传 async 回调时首个 await 处即提交，此后抛错不回滚。
- `worker_threads` 只是部分实现，隔离方案受限（worker 内跑 `bun:sqlite` 时主线程 maxGap 实测 1.3ms，但接口不完整）。
- Node 兼容按官方文档为 97 项通过、23 项黄色、3 项红色；`async_hooks` 的 `createHook`、`executionAsyncId` 是 stub，`cluster` 官方称 not battle-tested。
- `AsyncLocalStorage` 可用但不进入 `Bun.serve` 的 fetch/websocket handler，也不跨 Worker/MessagePort，因此请求上下文只能显式传参。
- `Bun.spawn` 的 IPC 仅兼容其他 Bun 进程（advanced 序列化），跨 Node 需退化为 `serialization: "json"`。
- Windows 官方列为支持，但 1.0 公告称 experimental，且无 tier 分级声明。
- 内置 SQLite 不是 Bun 独有：Node 26 的 `node:sqlite` 同样同步。

缓解：SQLite 后端按 `concurrency: 'single'` 声明并内部串行化事务（见 ADR-0003）；需要真异步存储时改用 Postgres 后端——`Bun.SQL` 的 Postgres/MySQL 适配器真异步，实测连接不可达的 2002ms 内事件循环 tick 39 次。
