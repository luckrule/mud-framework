# ADR-0007: 两阶段激活（prepare / activate）

## 背景

双进程架构里，Portal 需要在不中断玩家连接的前提下换掉 Server 进程。要做到这一点，替换进程必须先起来、再把旧进程退掉，最后才让新进程打开存储。旧实现只有一个 `ready` 屏障：Server 一收到 `hello` 就创建后端、注册 Extension、发 `ready`。于是"新进程已就绪"与"新进程已持有存储"是同一件事，替换顺序无从安排——新进程一启动就成了第二个写者（INV-002）。

ADR-0004 的崩溃重启路径不受影响：那个路径上新进程起来时旧进程已经退出。需要两阶段的是切换路径，而两条路径共用同一个启动协议，所以协议本身必须能表达"已就绪但还没碰存储"。

## 决定

### 握手：`hello` → `prepared` → `activate` → `ready` 是唯一启动路径

- Portal 发 `hello`（带 `portalGeneration`）。
- Server 回 `prepared`（带 `pid`，以及部署声明时的 `codeVersion`）。此时不创建后端、不校验能力、不注册 Extension、不安装请求处理器。
- Portal 发 `activate`。
- Server 才执行 `startServer`（后端、能力自检、降级告警、Extension 注册），随后安装请求处理器、置为 active，最后才发 `ready`。

普通启动与切换启动走同一条路径：`IpcExecutor.launch()` 收到 `prepared` 立即回 `activate`。切换路径将来只是在 `prepared` 与 `activate` 之间插入"旧进程 drain 并退出"这一步，协议形状不变。

### 这是破坏性的 IPC 协议变更，不做向后兼容

`prepared` 与 `activate` 是新增的必需消息，`hello` 不再直接触发 `ready`。旧 Portal 与旧 Server 都无法与新对端完成启动：旧 Server 收到 `hello` 就发 `ready` 并打开存储，新 Portal 会把它当作协议外的 `ready`；旧 Portal 不认 `prepared`，会在 `startTimeoutMs` 后失败。当前没有外部消费方，两侧同版本发布，因此不提供 `activation: 'immediate'` 之类的降级字段，也不保留"旧 Portal 视为立即激活"的分支——双模式会让"到底有没有打开存储"取决于对端版本，而 INV-002 不允许这种不确定。

### 边界：prepare 阶段不碰存储

`prepared` 的语义严格是"进程活着、能接受激活，但没有存储"。这保证：

- 一个 prepared 但从未被激活的进程不是写者，可以在旧进程仍持有存储时安全存在（INV-002）。
- 切换窗口内如果 Portal 决定放弃，直接关掉这个进程即可，不需要排空、回滚或关闭后端——没有东西可关。
- 激活失败（后端打不开、能力自检不过、必需 Extension 不满足）时 `startServer` 自行关闭它已打开的后端，进程以 `ServerStartupError` 退出；`prepared` 之后没有"半个写者"的中间态。

`codeVersion` 在回 `prepared` 之前校验：`prepared` 与 `ready` 携带同一个值，非法值会让 Portal 无法解析。

### 边界：切换前旧进程必须已经 drain 并关闭

协议只提供"准备好了"和"可以打开存储了"两个点，不规定谁在什么条件下发 `activate`。顺序责任在调用方：`activate` 只能在旧进程已停止接受请求、已排空在途请求、已关闭后端、已确认进程退出之后发出。ADR 先固定这条约束，避免下一阶段把"等旧进程"写成"等旧进程发 closed 就行"——`closed` 只说明 Server 完成了关闭流程，不说明进程已释放存储（ADR-0004 的退出等待就是为此存在）。

### 决定：`IpcExecutor.switchTo` 编排切换（顺序不可调换）

`switchTo({ spawn, codeVersion? })` 在 `status === 'ready'` 时启动一次切换，按固定四步执行，任一步的顺序都不能调换：

1. 用本次切换提供的 `spawn` 起新进程，发 `hello`，等到 `prepared`（有界：`startTimeoutMs`）。prepared 进程不碰存储。
2. 旧进程停止接收新请求（切换期间 `execute()` 仍接受并缓冲，不拒绝）；向旧进程发 `shutdown`（`timeoutMs = shutdownTimeoutMs`），等 `closed`，再等进程退出（`awaitPreviousClosed` 等 `closed`，`waitForExit` 等退出，必要时 kill 后再等一次）。
3. 确认旧进程消失后，才向新进程发 `activate`，等 `ready`（有界：`startTimeoutMs`）。
4. 把 `this.server` 切到新进程、`status = 'ready'`、`flush()` 把缓冲请求按 SPEC 6.5 投给新 generation；同时把本次切换的 `spawn` 与 `codeVersion` 记为当前版本。

理由就是 INV-002：新进程在 prepared 阶段不打开存储，`activate` 才打开。旧进程仍持有存储时 `activate` 新进程，就是两个写者。因此"等旧进程"必须是"已 closed **且**已退出"，不能只是 `closed`。步骤 2 与 3 之间没有可回退点：一旦发出 `activate`，新进程开始写存储。

同时只允许一个切换；进行中再次调用直接拒绝。切换期间 `status = 'switching'`，`execute()` 照常接受请求并留在未确认缓冲里，`flush()` 在 `switching` 期间不投递（旧进程正在被 drain，投给它等于把请求送进即将退出的进程）。

### 决定：两段式失败边界与回滚

- 旧进程 drain 之前失败（spawn 抛错、prepared 超时、hello 失败、新进程在 prepared 前死亡）：放弃并杀掉新进程，旧进程从未被打断，继续服务，切换 reject。
- 旧进程已 drain 并退出之后失败（activate 失败、ready 超时、新进程在 ready 前死亡）：此刻没有可用 Server。回滚为"用切换前的 spawn/codeVersion 重新启动"，复用现有重启路径（`status = 'starting'` → `launch`/`scheduleRestart`），并 reject 本次切换，保证缓冲请求不会永久挂起。重启前必须逐个 `waitForExit` 所有退出未确认的进程（旧进程与已 `activate` 的新进程），与崩溃路径同构（INV-002）。
- 旧进程在切换途中自己崩溃：切换失败，走同一重启路径；不把它的退出误记成普通崩溃并再排一次重启。
- 切换成功提升后，`spawn` 与 `codeVersion` 记为本次切换的目标值：此后崩溃重启的是刚提升的版本，新请求也盖上该版本（SPEC 6.4）。切换失败回滚则保持原版本。
- 任何失败路径都有界，且在被放弃进程的退出未确认前不会启动新进程；退出确认由回滚或 close() 各自完成。

`close()` 在切换期间必须能中止：中止信号会打断切换的每个 await，`doClose` 放弃新进程后按正常路径关闭旧进程，最终正常 `closed`。若被放弃的新进程已收到 `activate`（可能已持有存储），`doClose` 在返回前对它执行与 `shutdownServer` 相同的 `waitForExit` → `kill` → `waitForExit` 有界流程，仍未退出则记 warn。切换窗口内到达的请求最终要么在新进程成功，要么以稳定错误码结束（关闭时 `FRAMEWORK_SHUTDOWN`），不会挂到 deadline；回滚期间它们仍受自身 deadline 约束。

### 观测

切换开始（`fromCodeVersion`/`toCodeVersion`）、旧进程已 drain、新进程 ready、切换完成、切换失败与回滚原因各记一行。`ready` 的 `codeVersion` 与目标版本不一致时记 warn；`prepared` 可提前比对，同一次切换只告警一次。字段名沿用现有契约风格，不逐请求刷屏。

### 边界：父进程监控与 `ready` 重发

父进程存活轮询从 `begin()` 起就安装，与 `ready` 无关：prepared 阶段虽然没有存储可写，但一个父进程已被 `SIGKILL` 的 prepared 进程若无人告知，会永久存活下去。定时器 unref，不单独延长进程寿命。

prepare 阶段父进程消失时直接失败退出，不触碰存储：`run()` 停在 `await activation.promise`，所以 `onParentGone` 必须 reject `activation`（`ServerChannelClosedError`）并关闭链路，`run()` 才会返回。只 reject `completion` 不够——`run()` 那时还没走到 `await completion.promise`。若父进程在 `starting` 期间消失，则由在途的 `startServer` 收尾：它关闭刚打开的后端，`startAndServe` 以 `ServerChannelClosedError` 失败，不发送 `ready`。

`ready` 只在请求处理器安装完成、阶段置为 `active` 之后发送（`startServer` 不再自行发送 `ready`）。Portal 在收到 `ready` 后即可投递，发送方必须保证那时请求已能执行。

`hello` 的 generation 语义不变：首个 `hello` 只记录 generation 并发 `prepared`，不重发 `ready`（它本来就没有发过）；prepare 阶段收到更高 generation 时重发 `prepared`（新 Portal 没看到第一个屏障）；已 `ready` 后收到更高 generation 时重发 `ready`（沿用原有语义与告警）。

### 边界：关闭尚未 `ready` 的 Server

Portal 关闭一个尚未 `ready` 的 Server 走 kill，而不是 `shutdown` 消息：`IpcExecutor.shutdownServer` 只在 `server.ready` 时发 `shutdown`，否则直接 kill 并等退出。prepared 进程没有存储需要排空，也没有 `closed` 可回，消息往返只会拖长关闭。因此 Server 侧 `pendingShutdown` 分支（`shutdown` 在 `starting` 期间到达）只由直接构造的消息驱动——例如测试，或未来某个自己发 `shutdown` 的调用方——而不是普通 Portal 关闭路径。

## 后果

- 启动多一个 IPC 往返。普通启动因此多一次消息，代价固定且远小于后端初始化。
- 协议少了一处隐式保证："Server 活着"不再等于"Server 已就绪"。任何依赖 `hello` 之后立刻 `ready` 的调用方都要改；本仓库内只有 `IpcExecutor` 与测试替身。
- 切换路径的正确性从"进程启动顺序"变成"消息发出顺序"，可以在 `activate` 处加观测点，而不必靠进程启动日志推断。
- 切换窗口内请求延迟上升而非失败，代价由未确认缓冲（`maxPending`）兜底；切换失败时按上文两段边界决定"旧进程继续服务"还是"重启旧版本"。
- 仍未实现：`codeVersion` 到入口的映射、真实双版本集成。`switchTo` 只编排进程替换，不解释版本兼容性（SPEC 6.4）。
