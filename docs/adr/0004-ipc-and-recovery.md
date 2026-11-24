# ADR-0004: 进程间通信与恢复语义

## 背景

ADR-0002 决定了拆成两个进程，但没定两件事：两个进程之间用什么通道，以及"未确认"以什么事件为界。这两件事决定崩溃恢复能否成立——通道不自带消息边界，重放就得自己做分帧；确认若建立在"发送成功"上，恢复承诺就是空的。

候选通道有三个：Unix domain socket、stdio、`Bun.spawn` 的 `ipc` 选项。三者在 Bun 上的实测差异不是性能问题，而是正确性问题的来源：

- Unix domain socket 不做消息边界。三次独立 `write` 在接收侧被合并成一次回调，接收方必须自己做长度前缀，否则消息会被拼接或截断。
- stdio 没有分帧，且子进程任何一次 `console.log` 都会污染通道。
- `Bun.spawn` 的 `ipc` 选项自带消息边界，并可用 advanced 序列化。

## 决定

### 通道：`Bun.spawn` 的 `ipc` 选项 + advanced 序列化

只有它自带消息边界，另两个候选都得在框架里重写一遍分帧与转义。advanced 序列化保留 `Uint8Array`、`Date`、`Map` 的原型，往返实测 p50 0.071ms、约 40 万条/秒，二进制载荷不必先编码成文本。

它有三个已知缺陷。都不构成正确性威胁，但都约束调用方：

- **没有发送背压**：`send()` 恒返回 true。5 万条 4KB 消息使父进程 RSS 从 25MB 涨到 440MB。框架不能靠返回值判断对方是否跟上，因此内存约束只能由框架自己设，不能指望对端反压。这与 SPEC 3.5 的入站队列上限不是同一个东西：那条上限管的是尚未开始执行的请求，请求进入未确认缓冲后是另一份占用，具体分层见"后果"一节。
- **子进程退出瞬间可能丢消息**（oven-sh/bun#37849，未修）。因此确认 MUST 定义为"收到 `result`"，不能是"`send()` 返回"：丢失的消息仍在未确认缓冲里，重放会补上。这是确认语义不能建立在发送成功上的直接原因。
- **`ipc` 回调同步派发，返回值不被 await**。回调本身只做解析与入队，不做长同步工作，否则会直接变成事件循环上的延迟。但 `ready` 到达后回调末尾会同步调用一次重放：遍历未确认缓冲、按连接分组排序后逐条 `send`。积压时这是同步批量发送，规模受未确认缓冲上限（`IpcExecutor.maxPending`）约束，而不是逐条让出事件循环。

不选 Unix socket 还有两个环境理由：WSL 的 `/mnt/c`（drvfs/9p）下 `Bun.listen({ unix })` 抛 `ENOTSUP`；进程被 SIGKILL 后残留的 socket 文件不会自动 unlink，下一次启动直接 `EADDRINUSE`。stdio 则连"子进程不能往 stdout 写"这条都无法约束——业务代码一句 `console.log` 就能破坏通道。

### 恢复：重放缓冲在内存，确认以 `result` 为界

重放缓冲放内存，不落盘。SPEC 6.1 已明确"连接层重启会断开连接，但不会丢失已提交的业务状态"，即 Portal 崩溃允许丢未确认输入。落盘会把这份承诺扩大到 Portal 侧，代价是每次接收都要多一次持久化写入，而收益只覆盖 Portal 自身崩溃——不在承诺内。

内存缓冲必须有总数上限（`maxPending`，默认 10000），超出即以 `FRAMEWORK_QUEUE_FULL` 拒绝新请求。理由：缓冲规模本来由 deadline 自然收敛，但"Server 长期不可用而客户端持续写入"这一种情况里请求始终不被结算，deadline 管不住；通道又没有发送背压（见上），缺了这道上限内存增长就没有刹车。上限也不能取得太小：重启窗口内的批量输入会堆在这里，过小就会把正常流量误拒成队列满。

确认 = 收到 `result`。Portal 自己判定的终态（超时、重放耗尽、关闭、取消）直接销账：Portal 就是连接层，这些终态发生时结果已经交回调用方，不存在"已终态未确认"的窗口。反过来，Server 判定终态但 `result` 未送达时，请求仍在缓冲里，重放会再来一次——这正是 at-least-once 的来源。

框架不提供去重，幂等由业务负责（INV-011、SPEC 6.4）。ADR-0002 已定此点，这里只补一条可观测约定：重放时 `requestId` 不变、`replayCount` 递增，两者一起才能回答"这条日志是玩家哪一次输入的第几次尝试"（SPEC 7.3）。

重放上限 3 次（可配），超限进入终态 `FRAMEWORK_REPLAY_EXHAUSTED`。上限的存在理由是对称的：太少会让"重启窗口""握手超时"这类偶发失败也变成终态，3 次足以跨过它们；太多则把同一个坏输入变成对 Server 的持续冲击，而坏输入重放多少次都不会变好。

重启退避：首次立即，之后从 100ms 起按 2 倍递增（100、200、400…），上限 5000ms。重置条件是稳定运行超过 `max(30s, 请求超时)`，不是"收到 ready"。理由是实测：一个能起来但一处理请求就崩的 Server，若以 `ready` 重置退避，会在 300ms 内 spawn 275 次，退避形同不存在。稳定窗口必须同时长于退避上限与单请求最长存活时间，否则"起来、接第一个请求、崩"永远算第一轮。

重放顺序：按 `connectionId` 分组，组内按接收序号升序（SPEC 6.5）。已知代价是一个坏请求会饿死同连接上的后续请求，直到它的重放预算耗尽。这是顺序约束的必然代价，不是漏发：顺序保证与"跳过坏请求继续后面"不可兼得。

关闭预算：`IpcExecutor` 自己声明关闭的最坏耗时 `closeBudgetMs`，是四段串行等待之和，不能压成一个乘式：排空未确认请求（`shutdownTimeoutMs`）、等 Server 回 `closed`（`shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS`）、等进程退出（`EXIT_WAIT_TIMEOUT_MS`）、`SIGKILL` 后再等一次（`EXIT_WAIT_TIMEOUT_MS`）。默认值以 `ipc-budget.ts` 的常量为准：`shutdownTimeoutMs` 取 `DEFAULT_SHUTDOWN_TIMEOUT_MS`，另两个常量是 `EXIT_WAIT_TIMEOUT_MS` 与 `SHUTDOWN_ACK_SLACK_MS`。预算是 `closeBudget(shutdownTimeoutMs)`，即四段等待之和 `shutdownTimeoutMs + (shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS) + EXIT_WAIT_TIMEOUT_MS + EXIT_WAIT_TIMEOUT_MS`；这里不写死数值。外层 `ConnectionLayer` 关闭执行器时取自身 `shutdownTimeoutMs` 与该预算加少量余量的较大者，而不是各用一个独立超时。理由是这几段等待必须串起来算：外层若只按自己的 `shutdownTimeoutMs` 计时，`stop()` 会在 Server 仍在写存储时返回，接着重开存储就是两个写者（INV-002）。

预算单向派生：`shutdown` 消息携带 `timeoutMs`，由 Portal 声明自己愿意等 `closed` 多久；Server 的排空与关闭存储共享这一个 deadline，不再各自乘一次 `timeoutMs`，也不从自己的 `shutdownTimeoutMs` 推导。只有消息未带声明时才回退到 `4 × timeoutMs` 并记 `SHUTDOWN_BUDGET_UNDECLARED`。因此 Server 侧配置的 `timeoutMs` 不再拉长或缩短关闭时长。

`SHUTDOWN_ACK_SLACK_MS` 是对齐而非余量：Server 的计时从收到 `shutdown` 开始，比 Portal 的 `waitForClosed` 晚一个 IPC 跳。两侧取同一值时 Portal 的定时器总是先到，Server 用满预算就会被杀在正常关闭途中。

这段的旧公式是 `shutdownTimeoutMs × 2 + 退出等待上限 × 2`，与 Server 侧按 `4 × timeoutMs` 推导的预算不等；Server 的 `timeoutMs` 配得较大时，Portal 会 kill 一个正在正常关闭的 Server。这是修掉的真缺陷，不是可以简化的冗余：不要把这四段等待重新压回一个乘式。

### 结果消息的两个 Session 字段

`result` 上 `sessionId` 与 `requestSessionId` 语义不同：前者是 handler 在本次请求内新绑定的 Session，只有成功结果可能有；后者回显请求进入框架时已带的入口 Session，成功与失败结果都带，入口无 Session 时不带。加回显的原因是结果可能晚于 Portal 的结算（超时、取消、重放耗尽）：那时未确认缓冲里的条目已删除，Portal 只能从结果消息恢复 Session，而旧形状只覆盖"本次新绑定"，失败结果与"入口已有 Session 但未新绑定"的迟到结果都无从记录。`requestSessionId` 只属于线格式，不进入 `ExecutorOutcome` 或结算结果；旧 Server 不回显、旧 Portal 忽略未知字段，因此是加法兼容。`sessionId` 的语义不变。

### 确认的边界：框架做不到什么

这一节写清楚能力的真实边界，避免把"已确认"读成更强的保证。

- **取消不保证回滚**。`StorageBackend.transaction(fn)` 的提交发生在 `fn` resolve 之后。若取消或超时到达时 handler 已返回、COMMIT 尚未完成，框架没有回滚句柄，事务仍会提交。这个窗口由接口形状决定，关不掉。框架的责任是让它可见：记 `CANCEL_LOST_TO_COMMIT` 告警日志。要真正关掉需给 `StorageTransaction` 加 abort 能力，属破坏性接口变更（SPEC 8.2），当前不做。
- **超时与取消保证"结果确定且不再产生副作用"，不保证"handler 已停止"**。JS 无法抢占执行中的代码（SPEC 3.4）。框架能让后续写入失效、让结果不再送达，但不能让 handler 停下来。
- **Portal 的 deadline 是权威**。发给 Server 的 `timeoutMs` 是剩余预算而非总预算，且 Server 只能收紧不能放松（取 `min(Server 配置, 后端上限, 收到的值)`）。否则会出现 Portal 已超时结算、Server 仍在提交；而超时路径不重放，这比 at-least-once 更糟——写入存在，却没有任何重试会再次覆盖它。
- **业务错误码必须显式声明**。只有 `BusinessError` 的 `code` 与框架自身错误码进入 wire；驱动或运行时的偶然 `code`（`ENOENT`、`SQLITE_CONSTRAINT_UNIQUE`）不透传，统一降级为 `INTERNAL_ERROR`。否则第三方库升级会静默改变对外契约（INV-014）。

## 后果

获得：通道不自带分帧的代价由 Bun 承担；确认语义与消息丢失解耦，重放能补上退出瞬间丢的消息；重放与退避的边界写死，坏输入不会变成重启循环；框架能力的上限有明确记录，不必等到故障时才被发现。

代价与必须接受的限制：

- Portal 崩溃会丢未确认输入。这是 SPEC 6.1 允许的，但意味着"业务进程可崩"与"连接层可崩"的恢复强度不对称。
- 通道无发送背压，内存约束分两层：`ConnectionLayer.maxQueuedPerConnection` 只限制入站队列（尚未开始执行的请求）；请求进入 `IpcExecutor` 后转入未确认缓冲，其规模由"连接数 × 每连接在途数"决定，受 deadline 自然收敛，另有一个总数上限（`maxPending`）兜底。没有任何一层能感知对端的读取速度，上限缺失时内存增长没有刹车。
- 取消与超时可能以一次已提交的写入收场，只留下 `CANCEL_LOST_TO_COMMIT` 日志。消除这个窗口必须改存储接口，属破坏性变更。
- 同连接上的坏请求会阻塞后续请求直到重放预算耗尽，期间这些请求延迟上升，而不是快速失败。
- 通道仅兼容其他 Bun 进程（ADR-0001），跨语言或跨运行时的业务进程无法接入。
- `ipc` 回调在事件循环上同步派发，回调内的重活会直接变成请求延迟；这条约束只能靠代码纪律维持，框架无法在运行时检测。
