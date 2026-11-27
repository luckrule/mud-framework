# ADR-0007: 两阶段激活（prepare / activate）

## 背景

双进程架构里，Portal 需要在不中断玩家连接的前提下换掉 Server 进程：替换进程先起来、再退掉旧进程，最后才让新进程打开存储。只有一个 `ready` 屏障时，“新进程已就绪”与“新进程已持有存储”是同一件事，替换顺序无从安排——新进程一启动就可能成为第二个写者（INV-002）。崩溃重启不需要额外排序（新进程起来时旧进程已退出），但它与切换共用同一启动协议，所以协议必须能表达“已就绪但还没碰存储”。

## 决定

**产品约束（2026-11-26）**：部署期间不接受可感知的秒级延迟上升（受 `maxPending` 与 deadline 约束的短时有界缓冲属于设计内），也不接受玩家重连；因此保留两阶段激活与 `switchTo`。该约束撤销前不简化启动协议。这里只表达“不接受可感知的延迟上升”，不构成协议时限，也不是观测口径；具体阈值与观测方法属于部署验收与各自的实现配置，协议不承诺任何数字。

### 握手：`hello` → `prepared` → `activate` → `ready` 是唯一启动路径

Portal 发 `hello`；Server 回 `prepared`（带 `pid`），此时不创建后端、不校验能力、不注册 Extension、不安装请求处理器；Portal 发 `activate`；Server 才执行 `startServer`（后端、能力自检、降级告警、Extension 注册），随后安装请求处理器、置为 active，最后发 `ready`。普通启动与崩溃重启在确认没有旧写者后，收到 `prepared` 即发送 `activate`；`switchTo` 则在 `prepared` 与 `activate` 之间退役旧进程并确认退出，协议形状不变。

`prepared` 与 `activate` 是必需消息，`hello` 不再直接触发 `ready`。这是破坏性的 IPC 协议变更；不提供 `activation: 'immediate'` 之类的降级字段，双模式会让“到底有没有打开存储”取决于配置，而 INV-002 不允许这种不确定。

### 边界：prepare 阶段不碰存储

`prepared` 的语义严格是“进程活着、能接受激活，但没有存储”。一个 prepared 但从未被激活的进程不是写者，可以在旧进程仍持有存储时安全存在（INV-002）；切换窗口内如果 Portal 决定放弃，直接关掉它即可，不需要排空、回滚或关闭后端——没有东西可关。

激活失败（后端打不开、能力自检不过、必需 Extension 不满足）时，启动路径尝试关闭已打开的后端，并以 `ServerStartupError` 失败。已经发过 `prepared` 不代表后端初始化成功；已经发过 `activate` 的进程必须被视为可能持有存储，只有观测到退出才能解除写者屏障。

### 边界：切换前旧进程必须已经 drain 并关闭

协议只提供“准备好了”和“可以打开存储了”两个点，切换顺序由 `switchTo` 负责：先停止向旧进程投递请求，完成有界的排空与关闭等待，并确认进程退出，才能发 `activate`。`closed` 只说明 Server 完成了关闭流程，不说明进程已释放存储；kill 返回或退出等待超时也不是退出确认（ADR-0004）。发出 `activate` 前必须排除所有退出未确认的其他写者；屏障未满足时不得激活，也不以可用性为由放行（INV-002）。

### 决定：`IpcExecutor.switchTo` 编排切换（顺序不可调换）

`switchTo({ spawn })` 在 `status === 'ready'` 时启动一次切换，按固定四步执行：

1. 用本次切换提供的 `spawn` 起新进程，发 `hello`，等到 `prepared`（有界：`startTimeoutMs`）。prepared 进程不碰存储。
2. 旧进程停止接收新请求（切换期间 `execute()` 仍接受并缓冲，不拒绝）；向旧进程发 `shutdown`（`timeoutMs = shutdownTimeoutMs`），等 `closed`，再等进程退出（必要时 kill 后再等一次）。
3. 确认旧进程消失后，才向新进程发 `activate`，等 `ready`（有界：`startTimeoutMs`）。
4. 提升替换进程为当前 Server，把缓冲请求按 SPEC 6.5 投给新进程；同时采纳本次切换的 `spawn` 作为当前进程工厂。

新进程在 prepared 阶段不打开存储，`activate` 才打开；旧进程仍持有存储时 `activate` 新进程就是两个写者。因此“等旧进程”必须是“已 closed **且**已退出”，步骤 2 与 3 之间没有可回退点。同时只允许一个切换，进行中再次调用直接拒绝；切换期间 `execute()` 仍按容量与 deadline 接受请求并留在未确认缓冲里。切换开始、旧进程已 drain、新进程 ready、切换完成、失败与回滚原因各记一行。

### 决定：两段式失败边界与回滚

- 旧进程 drain 之前失败（spawn 抛错、prepared 超时、hello 失败、新进程在 prepared 前死亡）：放弃并杀掉新进程，旧进程从未被打断，继续服务，切换 reject。
- 旧进程已 drain 并退出之后失败（activate 失败、ready 超时、新进程在 ready 前死亡）：此刻没有可用 Server。回滚为“用切换前的 spawn 重新启动”，复用现有重启路径，并 reject 本次切换，保证缓冲请求不会永久挂起。重启前必须逐个确认所有退出未确认的进程（旧进程与已 `activate` 的新进程）已退出（INV-002）。
- 旧进程在切换途中自己崩溃：切换失败，走同一重启路径；不把它的退出误记成普通崩溃并再排一次重启。
- 切换成功提升后，`spawn` 记为本次切换的目标值：此后崩溃重启的是刚提升的进程工厂；切换失败回滚则保持原值。

任何失败路径都有界，且在被放弃进程的退出未确认前不会启动新进程。`close()` 在切换期间必须能中止：放弃替换进程后继续关闭原进程，对任何退出未确认的进程执行有界的“等退出 → kill → 再等”；仍无法确认时保留写者屏障并 reject 关闭，不能只记 warn 后宣称成功。等待超时但退出 Promise 仍 pending 时，迟到确认只允许在执行器尚可恢复时解除屏障；一次性 `observed: false` 不提供自动恢复依据。

失败以 `ServerSwitchError` reject，`details` 描述它留下的状态：`stage`（`preparing` / `retiring` / `activating`）、`previousServing`、`rolledBack`、`promoted`、`closeAborted`、`pendingExits`，以及 spawn 捕获到时的 `stderrTail`。这三个阶段与上面的边界一一对应，不是另起的分类；`closeAborted` 时 `previousServing` 与 `rolledBack` 都为 false，因为收尾已交给 `close()`，重试会被拒绝。理由：调用方要判断“能否立刻再切一次”，而文本 message 只说明失败原因，说明不了残留状态。

### 边界：父进程监控与 `ready`

父进程存活轮询从 `begin()` 起就安装，与 `ready` 无关：一个父进程已被 `SIGKILL` 的 prepared 进程若无人告知，会永久存活。定时器 unref，不单独延长进程寿命。prepare 阶段父进程消失时直接失败退出，不触碰存储：`run()` 停在 `await activation.promise`，必须 reject `activation` 并关闭链路；若父进程在 `starting` 期间消失，由在途的 `startServer` 关闭刚打开的后端，不发送 `ready`。

`ready` 只在请求处理器安装完成、阶段置为 `active` 之后发送；Portal 收到后即可投递，发送方必须保证那时请求已能执行。`hello` 只在进程等待握手时处理一次；重复的 `hello` 是协议外消息，记 warn 后忽略。

### 边界：关闭尚未 `ready` 的 Server

Portal 关闭一个尚未 `ready` 的 Server 走 kill 并确认退出，而不是等待 `shutdown` 确认。prepared 进程没有存储需要排空；已经 activate 但尚未 ready 的进程可能正在打开存储，两者都不能把 kill 返回当作资源释放。普通 Portal 不向尚未 ready 的进程发送 `shutdown`；Server 接受 starting 期间直接到达的 `shutdown` 并在启动收尾后关闭，但这不是普通 Portal 的关闭路径。

## 后果

- 启动多一个 IPC 往返，代价固定且远小于后端初始化；协议少了一处隐式保证：“Server 活着”不再等于“Server 已就绪”。
- 切换路径的正确性从“进程启动顺序”变成“消息发出顺序”，可以在 `activate` 处加观测点。
- 切换窗口内请求延迟上升而非失败，由未确认缓冲（`maxPending`）兜底；入口选择到进程工厂的映射属于应用与部署配置，不是 core 的职责。
