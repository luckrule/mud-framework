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

协议只提供"准备好了"和"可以打开存储了"两个点，不规定谁在什么条件下发 `activate`。顺序责任在调用方：`activate` 只能在旧进程已停止接受请求、已排空在途请求、已关闭后端、已确认进程退出之后发出。本阶段只实现"收到 `prepared` 立即 `activate`"这一普通启动适配；真正的 switchTo/drain 状态机属于下一阶段，届时 `activate` 的发出条件由它掌握。ADR 先固定这条约束，避免下一阶段把"等旧进程"写成"等旧进程发 closed 就行"——`closed` 只说明 Server 完成了关闭流程，不说明进程已释放存储（ADR-0004 的退出等待就是为此存在）。

### 边界：切换窗口内的请求缓冲与回滚

- 请求不投递给 prepared 的 Server。Portal 的未确认缓冲（`maxPending`）继续持有它们，等新 generation `ready` 后按原顺序重放（SPEC 6.5）。切换窗口因此不引入新的缓冲层，代价是这段时间的请求延迟上升而不是失败。
- 回滚即"不发 `activate`"。prepared 进程没有写过任何东西，放弃它不需要补偿；旧进程若还在服务，继续服务即可。已经发出的 `activate` 无法撤回——那一刻起新进程开始写存储，回滚退化为崩溃恢复路径（重放），不再是切换回滚。
- 请求在 `prepared` 与 `ready` 之间到达 Server 时（Portal 违约，或旧 Portal 的行为）一律忽略并记 warn：没有后端与 handler，执行无从谈起，也不能假装成功。`shutdown` 在 prepare 阶段直接关闭链路并完成，不打开后端。

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
- 未实现的部分（下一阶段）：`IpcExecutor.switchTo`、旧进程 drain/退出编排、失败回滚、`codeVersion` 到入口的映射、真实双版本集成。本 ADR 固定它们的边界，不实现它们。
