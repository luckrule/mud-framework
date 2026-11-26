# ADR-0004: 进程间通信与恢复语义

## 背景

ADR-0002 决定了拆成两个进程，但没定两件事：两个进程之间用什么通道，以及“未确认”以什么事件为界。通道不自带消息边界，重放就得自己做分帧；确认若建立在“发送成功”上，恢复承诺就是空的。

候选通道有三个：Unix domain socket、stdio、`Bun.spawn` 的 `ipc` 选项。三者的差异不是性能问题，而是正确性问题的来源：

- Unix domain socket 不做消息边界：多次独立 `write` 在接收侧可能被合并成一次回调，接收方必须自己做长度前缀。
- stdio 没有分帧，且子进程任何一次 `console.log` 都会污染通道。
- `Bun.spawn` 的 `ipc` 选项自带消息边界，并可用 advanced 序列化。

## 决定

### 通道：`Bun.spawn` 的 `ipc` 选项 + advanced 序列化

只有它自带消息边界，另两个候选都得在框架里重写一遍分帧与转义。advanced 序列化保留 `Uint8Array`、`Date`、`Map` 的原型，二进制载荷不必先编码成文本。它有三个已知缺陷，都不构成正确性威胁，但都约束调用方：

- **没有发送背压**：`send()` 恒返回 true，框架不能靠返回值判断对方是否跟上，内存约束只能由框架自己设。这与 SPEC 3.5 的入站队列上限不是同一个东西：那条上限管的是尚未开始执行的请求，请求进入未确认缓冲后是另一份占用。
- **子进程退出瞬间可能丢消息**。因此确认 MUST 定义为“收到 `result`”，不能是“`send()` 返回”：丢失的消息仍在未确认缓冲里，重放会补上。这是确认语义不能建立在发送成功上的直接原因。
- **`ipc` 回调同步派发，返回值不被 await**。回调本身只做解析与入队，不做长同步工作，否则会直接变成事件循环上的延迟。`ready` 到达后回调末尾会同步调用一次重放，规模受未确认缓冲上限（`IpcExecutor.maxPending`）约束。

### 恢复：重放缓冲在内存，确认以 `result` 为界

重放缓冲放内存，不落盘。SPEC 6.1 已明确“连接层重启会断开连接，但不会丢失已提交的业务状态”，即 Portal 崩溃允许丢未确认输入。落盘会把这份承诺扩大到 Portal 侧，代价是每次接收都要多一次持久化写入，而收益只覆盖 Portal 自身崩溃——不在承诺内。

内存缓冲必须有总数上限（`maxPending`），超出即以 `FRAMEWORK_QUEUE_FULL` 拒绝新请求。deadline 限制每条请求的驻留时间，包括 Server 尚未就绪或不可用的等待；它不限制同时到达的请求数量。通道没有发送背压，因此仍需总数上限控制突发输入与恢复窗口内的瞬时内存占用。上限不能代替 deadline，也不能保证重启窗口中的全部输入都被接受。

确认 = 收到 `result`。Portal 自己判定的终态（超时、重放耗尽、关闭、取消）直接销账：Portal 就是连接层，这些终态发生时结果已经交回调用方，不存在“已终态未确认”的窗口。Server 判定终态但 `result` 未送达时，请求仍在缓冲里，重放会再来一次——这正是 at-least-once 的来源。框架不提供去重，幂等由业务负责（INV-011、SPEC 6.4）；重放时 `requestId` 不变、`replayCount` 递增，两者一起才能回答“这条日志是玩家哪一次输入的第几次尝试”（SPEC 7.3）。

重放上限可配，超限进入终态 `FRAMEWORK_REPLAY_EXHAUSTED`。上限存在的理由是对称的：太少会让“重启窗口”“握手超时”这类偶发失败也变成终态；太多则把同一个坏输入变成对 Server 的持续冲击。重启退避首次立即，之后按指数递增到上限；重置条件是稳定运行超过一个同时长于退避上限与单请求最长存活时间的窗口，不是“收到 ready”。否则一个能起来但一处理请求就崩的 Server 会让退避形同不存在。重放顺序按 `connectionId` 分组，组内按接收序号升序（SPEC 6.5）；代价是一个坏请求会饿死同连接上的后续请求，直到它的重放预算耗尽——顺序保证与“跳过坏请求继续后面”不可兼得。

关闭预算：`IpcExecutor` 自己声明关闭的最坏耗时 `closeBudgetMs`，是六段串行等待之和，不能压成一个乘式：中止进行中的切换（`shutdownTimeoutMs`）、排空未确认请求（`shutdownTimeoutMs`）、等 Server 回 `closed`（`shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS`）、等进程退出（`EXIT_WAIT_TIMEOUT_MS`）、`SIGKILL` 后再等一次（`EXIT_WAIT_TIMEOUT_MS`）、对被 `close()` 中止的切换所放弃且已 `activate` 的替换进程再做一次“等退出 → kill → 再等”（`2 × EXIT_WAIT_TIMEOUT_MS`）。预算是 `closeBudget(shutdownTimeoutMs)`，即上述六段之和 `3 × shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS + 4 × EXIT_WAIT_TIMEOUT_MS`；常量以 `packages/core/src/shared/budget.ts` 为准，这里不写死数值。外层 `ConnectionLayer` 关闭执行器时取自身 `shutdownTimeoutMs` 与该预算加关闭余量的较大者，避免外层先放弃执行器的退出确认。完整 `ConnectionLayer.stop()` 的外部预算还必须覆盖另外两段串行等待：记 `C = ConnectionLayer.shutdownTimeoutMs`、`E = RequestExecutor.closeBudgetMs`、`S = EXECUTOR_CLOSE_SLACK_MS`，上界为 `C + max(C, E + S) + C`，依次是连接层排空、执行器关闭、传输关闭。没有声明 E 的执行器三段均使用 C；S 以 `packages/core/src/portal/connection-layer.ts` 为准。systemd 等外部停止预算应长于完整上界，而不是仅长于 E；关闭失败仍要尽力收尾传输，再传播失败，不能把预算耗尽解释为存储已释放。

预算单向派生：`shutdown` 消息携带 `timeoutMs`，由 Portal 声明自己愿意等 `closed` 多久；Server 的排空与关闭存储共享这一个 deadline，不再各自乘一次 `timeoutMs`，也不从自己的 `shutdownTimeoutMs` 推导。预算由 Portal 声明，Server 必须服从；缺该字段的消息被解析层拒绝，没有回退路径。`SHUTDOWN_ACK_SLACK_MS` 是对齐而非余量：Server 的计时从收到 `shutdown` 开始，比 Portal 的 `waitForClosed` 晚一个 IPC 跳；两侧取同一值时 Portal 的定时器总是先到，Server 用满预算就会被杀在正常关闭途中。

### 结果消息的两个 Session 字段

`result` 上 `sessionChange` 与 `requestSessionId` 语义不同：`sessionChange` 只在成功结果上出现，记录 handler 在本次请求内声明的路由变更，取值为 `{type:'bind',sessionId}` 或 `{type:'unbind'}`，没有变更时缺失；`requestSessionId` 回显请求进入框架时已带的入口 Session，成功与失败结果都带，入口无 Session 时不带。加回显的原因是结果可能晚于 Portal 的结算（超时、取消、重放耗尽）：那时未确认缓冲里的条目已删除，Portal 只能从结果消息恢复 Session，而只带 `sessionChange` 的形状覆盖不到失败结果与“入口已有 Session 但未新绑定”的迟到结果。`requestSessionId` 只属于线格式，不进入 `ExecutorOutcome` 或结算结果。

### 确认的边界：框架做不到什么

- **取消与超时不保证回滚**。`StorageBackend.transaction(fn)` 在 `fn` resolve 后提交；若取消或超时到达时 COMMIT 已开始但尚未完成，框架没有中止该 COMMIT 的句柄，事务仍可能提交，并记录 `CANCEL_LOST_TO_COMMIT` 告警。不增加提交中止能力；事务能力较弱时还须接受 SPEC 4.3 的降级。COMMIT 完成晚于 deadline 时，请求仍以超时终态结束，不返回成功、Session 变更或事件，也不回滚已提交内容；提交可能已经生效，并记录迟到提交告警。定时器未触发或结果已经算出都不是成功依据（SPEC 3.4）。
- **终态保证结果确定与事务句柄失效，不保证 handler 已停止或没有副作用**。成功、业务失败与框架失败后均拒绝保留句柄上的后续读写；正常成功不触发异常 abort。框架外副作用与已经开始的 COMMIT 不会因此撤销（SPEC 3.3、3.4）。
- **Portal 的 deadline 是权威**。发给 Server 的 `timeoutMs` 是剩余预算而非总预算，且 Server 只能收紧不能放松（取 `min(Server 配置, 后端上限, 收到的值)`）。否则会出现 Portal 已超时结算、Server 仍在提交；而超时路径不重放。

## 后果

获得：通道不自带分帧的代价由 Bun 承担；确认语义与消息丢失解耦，重放能补上退出瞬间丢的消息；重放与退避的边界写死，坏输入不会变成重启循环；框架能力的上限有明确记录。代价与必须接受的限制：

- Portal 崩溃会丢未确认输入。这是 SPEC 6.1 允许的，但意味着“业务进程可崩”与“连接层可崩”的恢复强度不对称。
- 通道无发送背压，内存约束分两层：`ConnectionLayer.maxQueuedPerConnection` 只限制入站队列（尚未开始执行的请求）；请求进入 `IpcExecutor` 后转入未确认缓冲，其规模由“连接数 × 每连接在途数”决定，受 deadline 自然收敛，另有一个总数上限（`maxPending`）兜底。没有任何一层能感知对端的读取速度。
- 取消与超时可能以一次已提交的写入收场，只留下 `CANCEL_LOST_TO_COMMIT` 日志。消除这个窗口必须改存储接口，属破坏性变更。
- 同连接上的坏请求会阻塞后续请求直到重放预算耗尽，期间这些请求延迟上升，而不是快速失败。
- 通道仅兼容其他 Bun 进程（ADR-0001），跨语言或跨运行时的业务进程无法接入。
