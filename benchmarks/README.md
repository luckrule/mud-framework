# Benchmarks

四个基准，都不进 CI 门禁，也不做性能断言：`bench:logger` 测 `safeLogger` 包装的每调用开销；`bench:game` 测 `examples/game-slice` 真实双进程装配下的热路径与空闲重启恢复；`bench:switch` 在真实 Portal + Server + WebSocket 上观测一次热升级期间的请求等待与切换耗时；`bench:load` 用独立的生成器进程驱动真实玩家级负载。`bench` 只聚合前两个，`bench:switch` 与 `bench:load` 单独运行：后两者需要更多资源，时长也由环境变量决定。

## 运行

```bash
bun run bench:logger
bun run bench:game
bun run bench:switch   # 版本切换时延，不包含在 bench 里
bun run bench:load     # 玩家级负载，不包含在 bench 里
bun run bench          # logger + game 微基准
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

方法：每组先 warmup，样本不含 warmup；延迟用客户端单调时钟测量，单位 ms（µs 精度），吞吐为 wall-clock ops/s；只报告观测值，不与阈值比较。退出时关闭所有客户端、SIGTERM Portal 并在有界时间内确认退出，必要时 SIGKILL；只有 Portal 停止成功（未走 SIGKILL 兜底）、退出已观测且没有幸存 Server pid 时才删除临时数据目录，否则保留目录并打印路径与原因（`cleanup: kept data dir ...`），同时非零退出。信号退出（拿不到退出码但进程已消失）且没有幸存 Server pid 时视为退出已观测，可以删除；走 SIGKILL 兜底（`forced`）一律视为非优雅停止，即使没有幸存进程也保留目录。`BENCH_KEEP=1` 保留目录，`BENCH_DATA_DIR` 指向自备目录时视为用户数据、不删除。只有装配、连接或清理失败才非零退出。

## 版本切换时延基准

`bench:switch` 在真实 Portal + Server + WebSocket 上执行一次热升级（`IpcExecutor.switchTo`），测的是客户端请求在切换期间实际等了多久，而不是只验证连接不断。

实现拆在 `benchmarks/game/switch/`：`portal.ts` 是基准 Portal，装配与 `examples/game-slice/src/portal.ts` 相同（`WsTransport` + `IpcExecutor` + `ConnectionLayer`），额外起一个只监听 `127.0.0.1`、端口由系统分配的 `POST /switch` 控制端点；同一时刻只接受一个切换，响应带服务端测得的 `switchTo` 耗时。`index.ts` 是负载、触发、分相统计与报告，`support.ts` 放配置、分相分类、Portal 生命周期与清理，`budget.ts` 按 ADR-0004 推导外部停止预算。Server 入口复用 `examples/game-slice/src/server.ts`，两个进程世代共用同一个临时 `GAME_SLICE_DATA_DIR`，替换顺序由框架保证旧写者退出后才激活新进程。

时间线：

- 客户端先登录，再按固定 start-to-start 间隔发 `game.look`（每个客户端同一时刻一个在途请求）。
- warmup 段不计入样本；随后先跑一段基线，再通过控制端点触发一次 `switchTo`。
- 切换窗口 = 触发 → `switchTo` resolve → 固定余量。请求按时间区间归类：在触发前完成的是基线，与窗口有重叠的是窗口（包括触发时已在途、切换结束后才返回的请求），窗口结束后开始的是恢复段。
- 报告服务端测得的切换耗时、`POST /switch` 往返时间、窗口跨度，以及基线/窗口/恢复三段的 p50/p95/p99/max、失败数、超时数、连接关闭数与错误 code 分布。超时单列：`timeout` 是客户端 `CLIENT_TIMEOUT` 与 Portal/Server 返回的 `FRAMEWORK_TIMEOUT` 之和，输出与摘要都在括号内分列 `client`/`framework`；`failed` 只统计两类超时之外的错误（业务错误、`INTERNAL_ERROR`、连接关闭等），框架超时既不会落入普通失败，也不会被 `timeout 0` 掩盖。
- 从 Portal 日志统计 `request replayed`、`request timed out` 与 `server switch completed`；报告所有 Server pid、当前 pid 和已退役 pid 是否已消失。
- 默认附带一个两客户端的 `say` 探针（`SWITCH_EVENTS=0` 关闭）：发送方的序号只在 `game.say` 被接受后推进，失败尝试复用同一序号。接收方按到达阶段统计 within-epoch 序号缺口与 duplicate。duplicate 可能来自“超时但已提交的 say 被重试”，与真实重复投递无法区分，不能当丢包；缺口只说明成功 say 的事件没有按序号到达。
- 所有 look 客户端也在 square，会收到探针的 `room.say`，但它们注册空监听器排空事件，不参与事件统计。

环境变量（默认值见 `benchmarks/game/switch/support.ts` 顶部常量）：`SWITCH_CLIENTS`、`SWITCH_INTERVAL_MS`、`SWITCH_DURATION_SEC`（客户端运行总时长，含 warmup）、`SWITCH_WARMUP_SEC`、`SWITCH_DATA_DIR`、`SWITCH_KEEP=1`、`SWITCH_EVENTS=0`、`REQUEST_TIMEOUT_MS`。同时设 `SWITCH_URL` 与 `SWITCH_CONTROL_URL` 可复用外部 Portal：跳过 spawn、Server pid 检查与数据目录清理，控制端点由外部 Portal 自己提供。

清理：关闭客户端、SIGTERM Portal、按 ADR-0004 的完整外部停止上界 `C + max(C, E + S) + C` 加 1000 ms 诊断余量等待 Portal 退出（当前装配 `C=5000`、`E=23250`、`S=1000`，即 34250 + 1000 = 35250 ms），超时才 SIGKILL；随后在有界时间内确认所有已见 Server pid 消失，必要时 SIGKILL。只有 Portal 优雅停止、退出已观测、没有幸存 Server pid 且运行没有失败时才删除临时目录。失败、`SWITCH_KEEP=1` 或 `SWITCH_DATA_DIR` 指定的用户目录都会保留现场并打印路径。只有装配、连接、切换被拒绝、cleanup 或“窗口/基线没有样本”才非零退出；请求失败、超时、掉线与事件缺口只报告，不设阈值。

限制：所有进程同机，切换耗时受本机进程启动、文件系统与调度影响；默认数据目录通常是 tmpfs，SQLite 与 Server 启动会比真实磁盘快；look 客户端与协调器同进程，客户端侧 JSON 解析与事件循环调度也算进请求延迟；结果是单机近似，跨机器没有可比性。结果不写入仓库文档。

## 玩家级负载基准

`bench:load` 与微基准的关键区别是客户端不共享协调器的事件循环：主程序 spawn 真实 Portal + Server，再 spawn 若干独立的 `benchmarks/game/load/client.ts` 生成器进程，每个进程只跑自己那份 WebSocket 客户端。客户端的 JSON 解析、事件分发与命令循环因此分散在多个进程里；单进程微基准里“所有客户端挤在一个 Bun 事件循环”的客户端侧瓶颈不会直接成为全局瓶颈。它仍是一台机器：生成器与框架争用同一批 CPU，时钟也是同机的，所以是同一主机上的近似，不是跨机器容量。

实现按职责拆在 `benchmarks/game/load/`：`index.ts` 是入口与阶段编排，`orchestrator.ts` 管 Portal/生成器生命周期与清理，`report.ts` 管聚合与报告，`resources.ts` 管 RSS/fd 采样，`client.ts` 是生成器进程，`support.ts` 放配置、类型、直方图与结果解析。

负载模型：

- 每个客户端登录唯一名字 `load-<generator>-<index>`，按 `LOAD_ROOM_SIZE` 分组落到 square/market/smithy/tavern；setup 阶段沿现有出口走到目标房间（复用外部 Portal 时也按登录返回的实际房间重新寻路），全部就绪后各生成器打印 `ready`，主程序汇总后通过 stdin 的 `go` 屏障统一释放测量。
- 每个客户端同一时刻最多一个在途请求。命令混合 `look` 60%、`who` 10%、`move` 20%、`say` 10%（`LOAD_MIX` 可改）。`game-slice` 没有独立的 `who` handler，交互客户端把 `who` 映射为 `game.look` 并只看玩家列表；负载生成器也把 `who` 记为 `game.look` 读路径，不伪造一个不存在的线上方法。`move` 在 square/market 之间往返；分到 smithy/tavern 的客户端在 setup 后先走到 market/square，因此不会撞无效出口。
- `realistic` 模式在每个命令后等待围绕 `LOAD_THINK_MS` 的随机 think time（抖动比例是脚本常量），模拟玩家节奏；`saturate` 不加 think time，找吞吐上限。`LOAD_DURATION_SEC` 是测量窗口，`LOAD_WARMUP_SEC` 的 warmup 在 `go` 之后、统计之前执行，不计入样本。
- `say` 的文本带唯一 token：run tag、发送者、房间 epoch、序号、发送时间戳与发送者当前房间。接收端按 token 统计从发送到 `room.say` 到达的延迟；发送方自己的响应延迟单独记在 `say` 命令延迟里。Server 的 `game.say` 只向发送者发送时所在房间的会话广播，所以收到的样本都算同房间投递；如果事件在队列里等待期间接收端已经走开，另记 `receiver-moved`，但样本仍计入延迟（剔除它会把“赶上换房间”的高延迟事件偏掉）。发送者每次成功换房间就把 epoch 加一。序号只在 `game.say` 成功后推进：被拒绝（如 `FRAMEWORK_QUEUE_FULL`）或超时的尝试不消耗序号，分别记入 `attempted`/`failed`，不计入 `sent`，因此不会在接收端留下序号空洞；`deliveries/say` 用成功发送数作分母——成功响应是可确认的广播集合，而超时但已提交的 say 可能也已广播，因此该比值可能大于 1。接收端只在 token 房间与本地快照一致时做缺口跟踪，在同一 (sender, epoch) 的成功序号序列内看到跳号才计为事件缺口，因此换房间与失败尝试都不会被误报成丢事件。超时但已提交的 say 可能已经广播，重试会复用同一序号，接收端把它记成 duplicate；duplicate 也可能是真实重复投递，两者无法区分，不能当丢包。重复投递、`receiver-moved` 与过期 epoch 分开计数。所有进程同机，`Date.now()` 可直接比较；事件延迟的分辨率因此是 1 ms。
- 延迟用固定桶直方图累计（5% 相对桶宽，覆盖 1 µs 到 60 s，另有溢出桶），p50/p95/p99 取命中桶的上界；精确的 min/max/均值/样本数另记。这样饱和档的几十万样本也不会撑大结果 JSON。ops/s = 成功命令数 / 各生成器测量窗口的并集（warmup 已排除）。错误按线上 `code` 计数，额外标出 `FRAMEWORK_QUEUE_FULL` 与客户端超时。
- 主程序每秒读 `/proc/<pid>` 采样 Portal 与 Server 的 RSS 与 fd 数，报告峰值与结束值；Server pid 从 Portal 日志的 `server process ready` 行解析，拿不到时跳过并注明。Linux 之外的平台跳过资源采样。
- 清理：先停采样，关闭生成器（SIGTERM、有界等待、必要时 SIGKILL），SIGTERM Portal 并在有界时间内确认 Portal 与 Server pid 消失，必要时 SIGKILL；只有 Portal 停止成功（未走 SIGKILL 兜底）、退出已观测且没有幸存 Server pid 时才删除临时目录与结果文件，否则保留并打印路径与原因（`cleanup: kept ...`），同时非零退出。信号退出且没有幸存 Server pid 时视为退出已观测；走 SIGKILL 兜底（`forced`）视为非优雅停止，即使没有幸存进程也保留目录。`LOAD_KEEP=1` 保留临时目录与结果文件；`LOAD_DATA_DIR` 是用户数据目录，不会删除。只有装配、连接或清理失败才非零退出；负载本身的超时、队列满与事件缺口只报告，不触发失败。

环境变量（默认值见 `benchmarks/game/load/support.ts` 顶部常量）：`LOAD_CLIENTS`、`LOAD_GENERATORS`、`LOAD_ROOM_SIZE`、`LOAD_DURATION_SEC`、`LOAD_WARMUP_SEC`、`LOAD_THINK_MS`、`LOAD_MODE=realistic|saturate`、`LOAD_MIX`、`LOAD_DATA_DIR`、`LOAD_URL`、`LOAD_KEEP=1`、`REQUEST_TIMEOUT_MS`。`LOAD_URL` 复用外部 Portal 时跳过 spawn、Server pid 采样与清理；此时数据目录由外部 Portal 自己决定，两个并发运行不要共用同一个数据库。

### tmpfs 与真实磁盘

默认数据目录建在系统临时目录下，Linux 上通常是 tmpfs；SQLite 的落盘成本被内存抹掉，写路径（`move`、`say` 的事务）会明显好于真实磁盘。比较写路径时把 `LOAD_DATA_DIR` 指到目标文件系统，例如仓库内已被忽略的 `benchmarks/.load-data`：

```bash
LOAD_DATA_DIR=benchmarks/.load-data bun run bench:load
```

同一档负载下对比两次的 `move`/`say` 延迟与 ops/s，`look`/`who` 主要反映读路径与队列，不应作为磁盘结论。数据目录放在仓库内时，`.gitignore` 已覆盖 `benchmarks/.load-data/`。

### 注意

- 生成器进程本身也会成为瓶颈：高并发、大房间广播时，单个生成器的事件循环可能先饱和。提高 `LOAD_GENERATORS` 可以把客户端分散到更多进程，但每个进程仍是一个 Bun 事件循环。
- 所有进程同机，事件延迟用 `Date.now()` 对齐；这只能作同机近似，跨机器绝对值没有可比性。延迟直方图给的是桶上界，不要把它当成精确分位数。
- 结果不写入仓库文档：数字随机器、电源策略、并发档位与运行时状态漂移，写死后会变成过时的性能承诺。正确性由测试与 CI 保证，基准不进门禁。
