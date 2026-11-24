# Benchmarks

两个可重复运行的基准，回答两个独立问题。它们只测量，不做断言：任何一次运行的结果都不是门禁，也不会写进 `docs/` 的承诺里。

## 测什么

### `bun run bench:logger`

`safeLogger` 包装的每调用开销，对照直接调用同一个 no-op logger：

- `info()`：1,000,000 次调用/样本，6 个 ABBA 配对（每侧 12 个样本）。配对按 raw,safe,safe,raw 与 safe,raw,raw,safe 交替，配对差值 = 对内 safe 均值 - raw 均值；报告 ns/call、ops/s、配对差值的中位数与范围，以及 ratio（仅当差值中位数与整个范围都大于 0 时）。
- `child()`：100,000 次调用/样本，4 个 ABBA 配对；框架每请求都会调用一次 `child()`。raw 侧每次调用都新建 logger（基准把返回值写到模块级变量，避免分配被优化掉），报告 ns/call、ops/s、配对差值的中位数与范围，以及 ratio。
- 附带的 `createJsonLogger` + 无 I/O sink：50,000 次/样本，4 个 ABBA 配对，用来给包装开销一个参照系。它测的是 `Date` + `JSON.stringify` 的成本，不是包装成本；主结论以 no-op 那组为准。该组打印 raw/safe 样本与逐对差值，但差值符号在多次运行间不稳定，所以只报绝对量，不给比值或占比。

耗时数值按 2–3 位有效数字取约数（如 `~5.9 ns/call`、`range 5.1 to 7.1 ns/call`），不打印两位小数伪装的精度。配对差值的中位数小于等于 0，或中位数大于 0 但范围跨零时，不报负成本、不报负占比，只输出 `not distinguishable from noise at this sample size` 和实测范围。

每组先跑独立 warmup（info 每侧 1,000,000 次、child 每侧 200,000 次、JSON 每侧 50,000 次），不计入样本。`child()` 的 raw 基线把返回的 logger 写到模块级变量，防止 JIT 判定返回值未使用而消掉分配；否则 raw 会塌缩成一次计数自增，分配成本会被错误地算进包装差值。

no-op logger 在方法里累加计数，采样后校验计数与调用次数一致。若运行时把调用消掉，计数对不上，基准直接失败而不是报告一个无意义的耗时。因此测到的是真实调用路径（闭包 + try/catch + logger 方法体），不是被优化掉的空循环。

### `bun run bench:e2e`

真实两进程装配的端到端延迟与吞吐：

- 本进程是 WebSocket 客户端；它启动一个真实的 Portal 子进程（`fixture-portal.ts`，真实 `ConnectionLayer` + `WsTransport`，端口 `0` 由 OS 分配，真实 `IpcExecutor`），Portal 再启动一个真实的 Server 子进程（`fixture-server.ts`，真实 `runServerProcess` + `SqliteBackend`，SQLite 临时库，WAL）。
- `echo`：框架边界。WS 帧解码、按连接排序、IPC 往返、请求事务（空事务）、编码。
- `store`：同一路径上加一条 `INSERT`，即真实 SQLite 写入与提交。
- 每轮先 warmup（默认 100，`BENCH_WARMUP` 可调），再顺序发 N 次请求（默认 1000，`BENCH_REQUESTS` 可调）。顺序请求意味着同一时刻只有一个请求在途：得到的是每请求延迟，不是饱和吞吐。
- 每模式每轮报告 p50/p95/p99、ops/s、总耗时；默认 4 轮（`BENCH_ROUNDS` 可调）取中位数并打印轮间波动。偶数轮让 `echo` 与 `store` 各先跑一半；传入奇数轮时 stderr 会提示顺序只抵消一半。
- 每轮使用新的临时目录与新的 Portal+Server；模式顺序逐轮交换，避免固定第二个模式总是吃到前一个模式预热好的页缓存。
- 输出 OS 内核版本、CPU 型号与临时目录的文件系统类型。识别到 `tmpfs` 时额外提示 fsync 近乎免费：此时 `store` 不包含真实磁盘的提交成本，要看落盘成本需把 `BENCH_DIR` 指到真实磁盘。
- 关闭时向 Portal 发 `SIGTERM`，等待框架自己的有界关闭，确认两个 pid 都消失。之后用另一个 `SqliteBackend` 读回行数，证明写入确实提交并越过了 Server 进程。失败时 SIGKILL 残留进程；没有进程存活就删除临时目录，杀不掉才保留并打印路径；`spawnPortal` 或读取 stdout 失败时也会删除刚建的临时目录，随后 `exit 1`。

## 怎么跑

```bash
bun run bench:logger
bun run bench:e2e

# 或用环境变量调整 e2e 的样本量与临时库位置
BENCH_REQUESTS=2000 BENCH_WARMUP=100 BENCH_ROUNDS=4 bun run bench:e2e
BENCH_DIR=/var/tmp bun run bench:e2e
```

`BENCH_DIR` 决定临时 SQLite 文件放在哪。默认 `os.tmpdir()`；多数 Linux 上 `/tmp` 是 tmpfs，`fsync` 近乎免费，`store` 的提交成本会被吃掉。要在意落盘成本时把它指到真实磁盘。报告头部的 `temp root: ... fs ...` 行给出实测文件系统类型，识别到 tmpfs 时会附一行提示。

默认值针对一台普通开发机：e2e 约几秒（4 轮、每模式 1000+100 次请求），logger 不到 1 秒。`bench` 依次跑两者。

## 为什么不在 CI

- 数字依赖机器：CI 共享 runner 的 CPU 频率、邻居负载、临时目录所在文件系统（tmpfs 与真实磁盘的 fsync 成本差一个数量级）都会移动结果，阈值断言只会带来假失败。
- 真实的子进程、端口、临时 SQLite 与多轮启动都比常规测试慢且更容易受环境影响；它们验证的是性能，不是正确性。正确性由 `tests/integration` 覆盖。
- 基准不是 `*.test.ts`，`bun test` 不会收集它们；仓库里有 `bench:*` 脚本，但 CI 与 pre-commit 不调用它们。

## 为什么文档里不写死数字

这里和 `docs/` 都不记录具体 ns/ms/ops。原因不是数字保密，而是它们会漂移：

- 同一份代码在不同机器、不同电源策略、不同文件系统上可以差一个数量级（`store` 的每次提交都受 fsync 支配）。
- 运行时、编译缓存与 JIT 状态会改变短循环的测量值；logger 基准用 ABBA 配对与配对差值的中位数/范围压低这种噪声，但压不掉跨机器的差异。
- 代码一改，旧数字就变成误导性承诺。`docs/SPEC.md` 承诺的是行为，不是性能。

需要判断是否回归时，用同一台机器、同一 Bun 版本，对改动前后的两次运行比较：`logger` 看配对差值的中位数与范围是否稳定地移动，`e2e` 看同模式的 p50/p95 与 4 轮波动是否覆盖了差异。跨机器的绝对值没有可比性。

## 读结果时注意

- `echo` 也会开一个请求事务（框架规定每个请求一个事务）。它测的是"框架边界 + 空事务提交"，不是纯内存转发。
- `store` 的成本由 SQLite 提交主导；WAL 与 `synchronous` 默认值没有改动，所以它反映默认配置，而非调优后的上限。临时库默认在 `os.tmpdir()`，若它是 tmpfs，提交几乎不落盘，`store` 与 `echo` 的差距会小到被噪声盖住；用 `BENCH_DIR` 指到真实磁盘才测得到 fsync 成本。报告头部的 `fs` 行与 tmpfs 提示说明本次运行实际用的是哪种文件系统。
- `logger` 的 JSON 组差值符号同样不稳定，输出只给中位数与范围；范围跨零时不把它当作成本，也不换算成占比。`child()` 的绝对值包含每次调用的对象分配与 GC，跨机器比较时优先看同一台机器上的差值与范围。
- `e2e` 的延迟包含客户端（本进程）与 Portal 各一次事件循环调度；`ops/s` 由总墙钟时间算出，顺序请求下它是延迟的倒数，不是最大吞吐。
- `startup` 行只用于观察每轮装配的启动漂移，不参与延迟统计。
- 失败时先看 stderr：fixture 的日志尾与错误都写在那里。临时目录在确认没有进程存活后删除；若有进程杀不掉，目录会保留并打印路径；启动子进程或读取其 stdout 失败时，刚建的临时目录也会被删除。
