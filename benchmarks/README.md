# Benchmarks

两个基准，都不进 CI 门禁，也不做性能断言：`bench:logger` 测 `safeLogger` 包装的每调用开销；`bench:game` 测 `examples/game-slice` 真实双进程装配下的热路径与空闲重启恢复。

## 运行

```bash
bun run bench:logger
bun run bench:game
bun run bench            # 两个都跑
```

`bench:game` 默认自己 spawn `examples/game-slice/src/portal.ts`：数据目录建在系统临时目录，`PORTAL_PORT=0` 由操作系统分配端口，从 Portal stdout 的监听行取实际地址。设 `BENCH_URL` 或 `GAME_SLICE_URL` 可复用已启动的 Portal，此时跳过 spawn、Server kill 与重启恢复，只测请求。

环境变量（默认值见 `benchmarks/game/index.ts` 顶部常量）：`BENCH_REQUESTS`、`BENCH_WARMUP`、`BENCH_CLIENTS`（逗号分隔的并发客户端数）、`BENCH_PER_CLIENT`、`BENCH_DATA_DIR`、`BENCH_KEEP=1`、`REQUEST_TIMEOUT_MS`。

## logger 基准

`bench:logger` 测量 `safeLogger` 包装的每调用开销，对照直接调用同一个 no-op logger。附带的 `createJsonLogger` + 无 I/O sink 只作参照：它测的是 `Date` + `JSON.stringify` 的成本，不是包装成本。

方法：

- 每组先独立 warmup，warmup 不计入样本。
- 每个测量对按 ABBA 交替（`raw,safe,safe,raw`，下一对反向），配对差值 = 对内 safe 均值 - raw 均值；线性漂移在同一对内相互抵消。
- 报告 ns/call、ops/s，以及配对差值的中位数与范围。只有中位数与整个范围都大于 0 才报告成本与 ratio；否则输出 `not distinguishable from noise at this sample size`，不把负差值当成提速，也不换算成占比。
- no-op logger 在方法里累加计数，采样后校验计数与调用次数一致；计数不符说明调用被运行时消掉，基准直接失败而不是报告无意义的耗时。
- `child()` 的 raw 基线把返回值写到模块级变量，避免分配被优化掉，否则 raw 会塌缩成一次计数自增。

## 游戏热路径基准

`bench:game` 驱动真实 Portal + Server：每个数都包含 WebSocket 一跳、Portal 路由、到 Server 的 IPC 一跳与 SQLite 事务。场景：

- `look` 顺序：单客户端，warmup 后逐次调用，报告延迟分布与 ops/s。
- `look` 并发：若干并发档位，每档各自登录一批客户端；每个客户端同一时刻只有一个在途请求，客户端之间并发。报告总吞吐与合并延迟分布，输出里注明 SQLite 单连接、Server 单写者会把请求串行化，每调用延迟包含排队。
- `move` 顺序：单客户端在 square/market 之间往返，覆盖写事务与同房间广播；目标房间放一个观察客户端，保证每次移动都有一个真实接收者。
- `say` + 事件：两个客户端同房间，A 发 `say`，B 收 `room.say`；分别报告 A 的响应延迟与 B 从发送前到事件到达的延迟。两侧时钟同一起点，事件时间在接收回调里取，不把 A 的响应尾巴算进 B。
- 空闲重启恢复：从 Portal 日志的 `server process ready` 取 Server pid；空闲时 SIGKILL，再以短间隔发 `look` 直到成功，报告 kill 到首个成功响应的时间与尝试次数。拿不到 pid 时打印跳过，不让整次基准失败。只测空闲恢复；在途请求的重放语义由 `tests/integration` 覆盖，基准不伪造该指标。

方法：每组先 warmup，样本不含 warmup；延迟用客户端单调时钟测量，单位 ms（µs 精度），吞吐为 wall-clock ops/s；只报告观测值，不与阈值比较。退出时关闭所有客户端、SIGTERM Portal 并在有界时间内确认退出，必要时 SIGKILL，确认 Server pid 消失后删除临时数据目录；`BENCH_KEEP=1` 保留目录，`BENCH_DATA_DIR` 指向自备目录时视为用户数据、不删除。只有装配、连接或清理失败才非零退出。

注意：

- `os.tmpdir()` 常是 tmpfs/内存盘，SQLite 的落盘成本被抹掉，写路径与重启恢复的绝对数字会明显好于真实磁盘；比较写路径时用 `BENCH_DATA_DIR` 指到目标文件系统。
- 所有客户端与基准脚本共享一个 Bun 事件循环，客户端侧序列化/解析也在被测时间内；跨机器、跨 Bun 版本的绝对值没有可比性。
- 结果不写入仓库文档：数字随机器、电源策略与运行时状态漂移，写死后会变成过时的性能承诺。正确性由测试与 CI 保证，基准不进门禁。
