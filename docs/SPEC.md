# SPEC

本文档定义框架的对外契约。代码是唯一真相；这里只写代码表达不了的部分——为什么这样选、边界在哪里。

强制程度用 MUST / MUST NOT / SHOULD 表达。未标注的段落是实现说明或理由，不构成契约。

## 1. 定位与承诺

### 1.1 定位

通用 MUD 游戏框架，运行于 Bun + TypeScript。框架提供让 MUD 业务安全运行的骨架：连接接入与有序化、请求事务边界、存储能力协商、业务进程生命周期与崩溃恢复、可观测的日志与错误码。游戏规则、命令解析与世界模型属于 Extension。

### 1.2 承诺

**P1 传输无关。** Transport（传输层）可整体替换，框架不绑定 WebSocket、TCP 或任何具体协议。
边界：框架规定请求边界与顺序，不规定 Transport 的内部编码、分帧与握手；连接层对客户端的线格式属于契约（§5.4）。新增传输只需实现传输接口，不得要求改动框架核心。

**P2 异步执行模型。** Handler 可以 await，框架的调度建立在 Promise 之上。
边界：框架不对 Handler 做隐式重试，也不假设 Handler 是纯函数；Handler 内部的无界等待由业务负责（§3.4）。

**P3 存储可插拔，但不假装等价。** 后端可替换，框架按后端的能力声明降级，不假装不同后端语义等价。
边界：声明之外的行为属于未定义。框架只承诺不会声称自己提供了后端没有的能力，不为后端的能力差异背书（§4.1）。

**P4 进程生命周期管理。** 连接与业务进程解耦，业务进程可替换、可重启，连接不因业务进程重启而断开。
边界：框架负责发现进程不在、重启并重放未确认输入；不负责业务进程内部状态在重启后的正确性，那属于业务的幂等设计。

**P5 可观测。** 框架产出结构化日志与稳定错误码。
边界：稳定的是错误码与日志字段名，不是日志文案；框架不承诺指标与追踪的具体后端。

## 2. 核心术语

代码标识符、日志字段与文档 MUST 使用本节的定义，不引入同义词。类型名、类名、进程名与导出函数名是例外：它们标识实现构件，允许与术语不同，但每个不同的名字 MUST 在本节内显式声明它对应哪个术语，且同一概念 MUST NOT 有两个实现名。

**Request（请求）** — 客户端输入的最小处理单元，也是顺序保证与事务边界的基本单位。一条输入对应一个 Request；框架不把多条输入合并成一个 Request。

**Connection（连接）** — 一条具体传输链路上的连接实例。Connection 断开不等于 Session 结束。

**Session（会话）** — 玩家的一次逻辑在线期，由 Session ID 标识，可跨越多个 Connection。Session 是路由与状态归属的单位。连接层保存 `sessionId → connectionId` 活跃路由；绑定与解绑由业务通过 `RequestContext.bindSession` / `RequestContext.unbindSession` 声明，只在请求成功时生效。框架要求非空 id，不判断其业务合法性。一个 Connection 同时只持有一个 Session；新 Connection 接管同一 Session 后，旧 Connection 保持连接但不再持有该 Session，只有再次显式绑定才会重新获得身份。连接断开时清除其路由，但 MUST NOT 因此丢弃该 Session 尚未结束的请求屏障（§3.2）。边界：框架不持有 Session 的业务状态，其保留窗口与结束规则归业务进程。

**Transport（传输）** — 传输实现的接口：接受连接、交付入站数据、写出发站数据、关闭。它只负责字节流与传输帧的编解码，不含 Envelope 编解码、Session 路由与输入缓冲；Envelope 与客户端线格式见 §5.4，其余职责属于连接层进程。框架核心不依赖任何具体 Transport。

**Handler（处理器）** — 处理单个 Request 的业务函数，可以是 async。输入是已解码的请求与请求上下文；正常返回即表示该请求成功。注册时声明 `access`（默认 `'session'`，即要求请求携带 Session；`'public'` 不要求）与 `mayChangeSession`（默认 false，只有声明 true 的 Handler 可以调用 `bindSession` / `unbindSession`）。

**Extension（扩展）** — 业务能力的打包单元，向框架注册 Handler 并声明自己依赖的存储能力。框架核心不含任何游戏规则。形状：`name`（日志中的唯一标识）、`requires`（存储能力最低要求，省略即在任何后端上运行）、`required`（是否为本进程启动所必需）与 `register(registrar)`。registrar 由框架提供，自动标注 handler 所属的 Extension，使请求日志能带上它（§7.1）。

**Storage Backend（存储后端）** — 实现存储契约并给出能力声明的持久化实现，以独立包发布（§4）。

**Capability Declaration（能力声明）** — 后端自述支持哪些存储能力的数据结构，框架据此选择降级路径（§4.2）。

**Business Process（业务进程）** — 承载 Extension、Handler 执行与存储访问的进程，与连接层进程分离，是存储的唯一写者。

**Connection Process（连接层进程）** — 只负责传输接入、Session 路由与输入缓冲的进程。它不执行 Handler，不访问存储。

实现命名：Connection Process 在实现里叫 **Portal**，Business Process 叫 **Server**（装配见 ADR-0002 与 `docs/INTEGRATION.md`）。

**Input Queue（输入队列）** — 每个 Connection 上的有界请求队列，保证同连接内请求有序（§3.5）。

**Request Context（请求上下文）** — 随 Request 传入 Handler 的框架侧上下文，携带 Session ID、Connection ID、请求标识、事务句柄、中止信号、日志器与 Extension 名。Handler 的存储调用与日志调用 SHOULD 经由它。

`bindSession` 与 `unbindSession` 只在上下文中记录变更，一个请求至多一次；变更仅在请求成功且未超时时交给调用方，由连接层在结果返回后应用。失败、超时或取消的请求不返回变更，因此失败的登录不会留下半建成的路由，失败的登出也不会移除已有路由。

中止信号是标准 `AbortSignal`，超时、取消或断线时 abort，`reason` 说明触发原因；正常成功不会触发异常中止。它是通知而非抢占：handler 可把它传给接受信号的操作以提前停下。事务句柄在请求的任一终态后均失效，不依赖 handler 是否响应信号；这不撤销已经开始的 COMMIT 或框架外副作用（§3.3、§3.4）。

**Terminal State（终态）** — Request 的结局，只有三类：成功、业务失败、框架失败（超时、取消、存储不可用、传输在完成前断开）。终态表示处理结束、结果已确定；它不等于已确认。

**Replay（重放）** — 业务进程崩溃后，把已接收但未确认的 Request 重新投递给重启后的业务进程（§6.4）。

**Acknowledged（已确认）** — Request 的终态结果已交回连接层的状态。终态与确认之间有一个窗口：结果已确定但尚未交回连接层时进程崩溃，该 Request 仍是终态却未确认。恢复语义基于确认而非终态——只有已确认的才能从重放缓冲中移除（§6.4）。

## 3. 执行模型

### 3.1 顺序

同一 Connection 上的 Request MUST 按接收顺序推进：Request N 到达框架终态后，N+1 才能开始处理。成功终态必须发生在该请求的事务提交之后；超时、取消与已开始 COMMIT 的竞态边界见 §3.4。理由：玩家的连续指令依赖前一条的结果；框架不规定顺序的实现方式，只规定可观测的结果。

### 3.2 并发

不同 Connection 之间 MUST 并发处理。

同一已绑定 Session 的框架非终态请求 MUST 串行处理。请求按开始处理时的入口 Session 归属；旧 holder 被接管后，其新请求不再携带原 Session，已开始请求的入口身份不变。新 holder 上属于该 Session 的请求 MUST 等已有请求的 executor Promise 到达终态，不得以等待超时为由并发放行。

Session 的终态屏障独立于活跃路由：连续接管、旧连接断开或路由清除均 MUST NOT 丢弃尚未满足的先决屏障。绑定在成功结果返回后应用，不追溯改变本次请求的入口归属；首次从无 Session 发起的绑定请求不在目标 Session 的串行保证内。业务必须自行保护这种登录或绑定逻辑的共享状态。

Session 变更只作用于发起请求的 Connection：若目标 Session 已被新 Connection 接管，旧 Connection 的成功 unbind MUST NOT 解除新 holder 的路由，并记录一条 warn（`SESSION_CHANGE_DROPPED`，字段含 `connectionId`、`sessionId`、`reason`）。

Session 状态转移的契约：

- 活跃路由只在成功请求的 bind/unbind 或连接断开时改变；失败、超时、取消与关闭结算不改变路由。
- takeover 只改路由，不清除任何未完成的终态屏障；屏障只由请求开始与终态改变，与路由独立。
- 旧 holder 的迟到 unbind 不影响新 holder；迟到 bind 是显式登录，可重新接管路由，但仍须等待既有屏障。
- 已断开的 Connection MUST NOT 获得新路由；其迟到结果按取消处理，不应用 Session 变更。
- 同一请求至多一次 Session 变更；第二次调用被拒绝，首次变更仍是唯一待应用变更。
- 入口无 Session 的 public 请求执行 unbind 是成功的 no-op：路由不变，结果仍声明 unbind，因此无法区分“本来没有”与“已解绑”。

边界：终态不等于 JS handler 已停止。超时或取消后，非合作 handler 可能继续计算，其框架外副作用与已开始 COMMIT 的竞态不在串行保证内（§3.4）。框架不提供跨 Connection 的事务；需要跨连接一致性的场景由 Extension 使用存储层的并发原语处理，并自行承担冲突。并发承诺止于存储边界（§4.4）。理由：跨连接串行会抹掉异步模型的全部收益；同 Session 并行则让“谁的状态更新生效”变得不可判定。

### 3.3 事务边界

每个 Request 隐式包在一个事务里，Request 与事务一一对应：

- Handler 正常返回 → 提交。
- Handler 抛错、超时或被取消 → 回滚。
- 同一 Request 内的多次存储调用 MUST 共享同一事务上下文，不得各自独立提交。

上述提交与回滚以存储后端声明的事务能力为限（§4.3）。取消或超时遇到已经开始的 COMMIT 时不能保证回滚（§3.4）。

`RequestContext.storage` 只在本 Request 未到终态时有效。成功、业务失败或框架失败后，保留的事务句柄 MUST 拒绝后续读写，MUST NOT 借后续 Request 的事务继续操作；事务句柄失效不意味着正常成功要触发 `AbortSignal`。

Handler 内 MUST NOT 开启新事务。需要多段原子性的业务应拆成多个 Request。理由：Request 是业务能理解的原子单位；让业务手写事务边界会把事务管理成本扩散到每个 Handler。

### 3.4 超时与无界等待

每个 Request MUST 有超时上限，由框架配置，不由 Handler 自行决定。实际生效的超时以 `min(框架配置, 后端声明的 maxTransactionDurationMs)` 为上限：后端声明的是它自身能承受的上限，框架不得超过它；调用方的剩余预算只能在此基础上再收紧，不能放宽。

超时后的语义：

- 超时到达即把该 Request 置为终态（框架失败，`FRAMEWORK_TIMEOUT`），并使其事务上下文失效。
- 此后通过该上下文发起的读写 MUST 被拒绝，不得进入存储层。
- 框架 MUST NOT 继续等待该 Handler，Connection 可以立即处理下一个 Request。
- 未进入提交阶段的事务在 handler 返回后尝试回滚，强度以 §4.3 的后端声明为限；超时不保证事务已回滚或 handler 已停止。

JS 无法抢占执行中的 Handler。超时保证结果确定与后续事务调用失效，不撤销已经发生的写入，也不保证框架外副作用停止。`RequestContext.signal` 同时 abort（`reason` 为 `'timeout'`），只是提前停止通知。

已经开始的 COMMIT 是明确边界：`StorageBackend.transaction(fn)` 在 `fn` resolve 后提交，取消或超时可能发生在提交完成之前，框架没有中止该 COMMIT 的句柄。此时事务仍可能提交，并记录 `CANCEL_LOST_TO_COMMIT` 告警（ADR-0004）；MUST NOT 把超时或取消当作“未生效”的证明。

COMMIT 完成时刻晚于 deadline 时，请求仍以超时终态结束：不返回成功、Session 变更或事件，也不回滚已提交内容。提交可能已经生效，框架记录迟到提交告警（`CANCEL_LOST_TO_COMMIT`）；定时器是否触发、结果是否已经算出都不是“按时完成”的证明（ADR-0004）。

事务内 MUST NOT 出现无界等待，包括：等待另一个 Request 的结果、等待用户输入、等待无超时的外部网络调用、等待一个无上界的锁。理由：事务持有后端资源或锁，一次无界等待会把单请求超时放大成整条连接乃至整个后端的停顿。

### 3.5 有界队列

每个 Connection 的 Input Queue MUST 有明确上限，上限由 `ConnectionLayerOptions.maxQueuedPerConnection` 定义（默认值见选项定义）。队列满时 MUST 显式拒绝新输入并返回 `FRAMEWORK_QUEUE_FULL`，MUST NOT 静默丢弃——静默丢弃会让玩家指令凭空消失且不可诊断。

选择拒绝而非背压：拒绝是队列自己能兑现的唯一策略，让请求立刻以稳定错误码失败，而不是把整条连接堵在一个满队列后面。

`FRAMEWORK_QUEUE_FULL` 同时表示 Portal 的未确认缓冲达到上限（`IpcExecutorOptions.maxPending`）。两层队列的拒绝语义一致：以同一稳定错误码立即失败，不静默丢弃。

事件缓冲的上界见 §3.8。它与本节 Input Queue 的上界是两个独立的界：分别计量、分别执行各自的溢出策略，不合并为一个总上限。

### 3.6 取消与断线

Connection 在 Request 处理中关闭时，该 Request MUST 进入终态，不得无限期挂起。

策略是取消，没有例外：中止信号触发，事务上下文失效，Request 以框架错误结束。未进入提交阶段的事务在 handler 返回后尝试回滚；已经开始的 COMMIT 仍受 §3.4 的边界约束。选择取消的理由是玩家已无法接收结果，不能让后续事务调用继续生效。

本节的取消只针对客户端或传输发起的关闭。连接层因事件缓冲积压主动断开时（§3.8）不取消已开始的请求：它们的终态不变，照常结算，只是结果可能写不出去；该连接上尚未开始的请求不执行。两条规则不冲突，因为积压断开是框架自己的背压决定，不是玩家离开——把已提交的工作当作取消处理，会撤销玩家已经发生但只是没读到的状态变化。§3.8 的「写出调度与有界缓冲」段落给出完整语义。

框架不提供不可取消声明。Portal 在断线时结算请求并清除连接持有的路由；若 Server 可拒绝取消，Portal 就需要额外的取消确认协议才能区分“仍在执行”与“已到终态”，否则重连后的 Session 屏障无法据此决策。

需要不可回滚副作用的业务 MUST 自行保证幂等与重试安全，不能把断线取消或超时当作副作用未发生的证明。

断线不等于 Session 结束。Session 状态保留多久、何时结束由业务自行决定并实现；期间重连可恢复。

### 3.7 访问检查

Handler 的访问检查在业务进程内、Dispatcher 上执行，顺序是契约：

1. 未注册的 method 以 `UNKNOWN_METHOD` 结束，不因请求缺少 Session 改成 `AUTH_REQUIRED`。
2. Handler 默认 `access: 'session'`；请求的入口 Session 缺失或为空时，以 `AUTH_REQUIRED` 拒绝并记录 warn（`code`、`method`）。`'public'` 跳过这一步。
3. 若消费方注入了 `authorize(ctx, method, payload)`，对所有已注册 Handler（包括 public）调用；返回 false 以 `FORBIDDEN` 拒绝并记录 warn，抛出的异常按框架内部失败处理（`INTERNAL_ERROR`）。策略只读取请求身份、`AbortSignal` 与日志器，不获得存储或 Session 变更能力，且在请求 deadline 内完成。
4. 打开存储事务并执行 Handler。

拒绝 MUST NOT 打开存储事务、执行 Handler 或改变 Session 路由。Handler 未声明 `mayChangeSession: true` 却调用 `bindSession`/`unbindSession` 时，校验抛出普通错误；handler 未捕获时请求以 `INTERNAL_ERROR` 失败；handler 捕获后请求可以成功，但不会记录任何 Session 变更——吞掉该错误不构成合法用法。检查点与拒绝语义属于框架；认证与授权策略、登录流程属于消费方，由消费方注入或实现为 public Handler。

### 3.8 Server 主动事件

Handler 可以为一个请求声明要发送的事件（ADR-0008）。`ctx.emit(event, payload)` 发给当前请求的 Connection；`ctx.emitToSession(sessionId, event, payload)` 发给该 Session 在实际写出时的当前 holder。事件名与 payload 对框架不透明；事件名 MUST 非空，`emitToSession` 的 Session id MUST 非空。

事务边界：

- 事件在 Handler 内只进入本请求的缓冲，MUST 在请求成功提交后才发送；同一请求 MUST 先发结果、再发事件。
- Handler 抛错、请求超时、被取消或事务回滚时，缓冲的事件 MUST 丢弃，MUST NOT 发送。
- 请求到达终态后再声明事件 MUST 被拒绝，与事务句柄的失效边界一致。
- 事件发出后不回滚。投递是 best-effort：可能因目标缺失、连接关闭、编码或写失败而丢失；请求重放可能让同一事件以相同 `eventId` 重复发送。框架不提供投递确认或去重，需要去重的业务可用 `eventId` 自行判断。
- 每个请求可缓冲的事件数量 MUST 有界，上限由 `ServerProcessOptions.maxEventsPerRequest` 定义，默认 1024；`ctx.emit` 与 `ctx.emitToSession` 计入同一个上限，计数按请求重置。该选项 MUST 是大于等于 1 的安全整数，在启动期校验；不合法时 Server MUST 拒绝启动。
- 超出上限时声明事件的调用抛出普通错误，错误信息 MUST 带实际配置的上限值；Handler 可以捕获。该错误未逃逸时请求仍可成功，逃逸时按 Handler 失败处理（wire 上为 `INTERNAL_ERROR`）。一次广播可声明的事件数同样受该上限约束：目标 Session/收件人数超过上限时，超出部分由 Handler 自行取舍或分批发送。

投递语义（best-effort）：

- 目标 Connection 不存在或已关闭、目标 Session 当前没有 holder 时，事件 MUST 被丢弃并记录 warn（`EVENT_DROPPED`）；编码失败或传输写失败时同样丢弃并记录 `TRANSPORT_SEND_FAILED`。MUST NOT 因失败而缓冲或重试，也不得让请求失败。
- Session 目标的收件人按实际写出时的当前 holder 解析：排队期间 takeover 会改投新 holder，至多重投递一次，再次变化即丢弃。
- 同一 Session 的 session 目标事件 MUST 按发射序串行投递：后一条必须等前一条写出或丢弃后才开始，因此 takeover 不改变同一收件 Connection 上这些事件之间的顺序。投递分两段，只有第一段可取消：**queued**（尚未开始写出）时旧连接被移除或 abort，该事件丢弃（`EVENT_DROPPED`，reason 指明写出前连接已关闭）并立即释放串行位；**writing**（已开始写出）时 abort MUST NOT 提前释放串行位，必须等实际写结果（成功或传输失败）结算后再释放，写失败记 `TRANSPORT_SEND_FAILED` 而不是 `EVENT_DROPPED`，因为帧可能已写出。写出本身必须有界：传输发送受其 `sendTimeoutMs` 约束，底层发送 Promise 不保证 settle 时以发送超时为上界，不得用 abort 信号代替。这个写出上界的取值顺序是：连接层的显式 `eventSendTimeoutMs` 配置原样采用；否则取传输声明的单帧发送上界（§5.2）加一段固定余量；传输声明不可用（例如 `Infinity`）时按未声明处理，回退到默认值。显式值 MUST 是不小于 1 的有限数且不超过 Bun `setTimeout` 可接受的最大延时（2^31−1 ms）；超过时 `ConnectionLayer` 构造抛错，MUST NOT 钳制——Bun 会把超过上限的定时器钳为 1ms，最宽的界反而变成最窄的。余量不是给传输更多时间，而是让连接层的界在 Bun `setTimeout` 可接受的最大延时（2^31−1 ms）以下严格宽于传输的界：两者等值时连接层的兜底计时器与传输自己的计时器几乎同时到期，且常先触发，日志便只剩「传输没有按承诺 settle」，看不出是传输卡死还是传输的超时机制本身失效；触顶时以封顶值为准。只有连接层的界更宽，传输自己的失败判定才是可观测的那个。默认值不加余量：此时没有传输计时器可让位，默认值本身就是唯一判定。显式配置也不加余量：它替换推导结果，而不是与传输计时器竞争。
- 结果发送失败时，该请求的全部事件 MUST 跳过并记录；请求的成功状态不变。
- 结果帧的写出同样 MUST 有界，上界与事件帧同源（§5.2，取值顺序同本节上一条）。传输的发送 Promise 不保证 settle，无界等待会让该连接的单写者永不推进，并让承载该请求的 Session 终态屏障永久持有（INV-004），其他连接上针对该 Session 的请求随之无限等待。超过上界时 MUST 记录 `TRANSPORT_SEND_FAILED` 并继续，不抛错：请求的成功状态不变，也不承诺发出响应（§5.4）。这个界同样约束不经过连接调度器的准入拒绝与关闭拒绝帧（§5.4）；它们若阻塞，卡住的是该连接的输入处理。
- 事件帧之间的发射序按投递路径界定：同一 Session 的 session 目标事件由按 Session 的串行队列保证，同一 Connection 上的 connection 目标事件按发射序写出；connection 目标与 session 目标之间、不同 Connection 之间不承诺发射序。结果帧与事件帧之间的相对顺序见本节“写出调度与有界缓冲”。
- 事件是附加通道：MUST NOT 改变请求的终态、超时、取消、重放、Session 屏障或关闭语义。

写出调度与有界缓冲：

- 连接层持有的每一处事件缓冲（每 Connection 的事件队列、每 Session 的待投递队列）MUST 有界：已接收、尚未写出的事件帧数与字节数都不得越过上限，上限由连接层选项 `maxQueuedEvents` 与 `maxQueuedEventBytes` 定义，默认值见选项定义。字节数按事件帧经当前 `Codec` 编码后在 wire 上的字节数计，不按字符数计。上限是每个队列实例的上界，不是每 Connection 汇总的上界，也不是进程总量的上界：各队列的上界互不推导，全部缓冲合起来能占多少内存取决于同时积压的队列数，由部署方按 §6.1 的连接数上限与这两个选项自行约束。达到任一上限时 MUST 以 close code `1013`（Try Again Later）断开该缓冲当前的持有连接（Session 待投递队列为当时的 holder）并记录（`EVENT_BACKLOG_DISCONNECTED`，字段见 §7.1），MUST NOT 静默处理，MUST NOT 因此改变请求终态或让请求失败。Session 待投递队列没有 holder 时无从断开，该事件按「目标缺失」丢弃并记录（§7.1）。断开是框架主动关闭，不是客户端离开：该连接上已开始的请求 MUST 继续结算、不取消执行（§3.6 的取消策略只覆盖客户端或传输发起的关闭），其结果写不出去时记 `TRANSPORT_SEND_FAILED`，受同一个写出上界约束（§5.4）；尚未开始写出的队列内事件帧与尚未开始的请求 MUST 不再写出或执行。断开在决策时刻生效，不等传输回报关闭：排队中的请求与事件帧据此判定，否则传输的关闭事件晚于写者的下一轮调度时，会有一个请求在已决定断开的连接上开始执行。理由：队列满说明客户端持续读不动，静默丢事件会让玩家在已经失真的世界里继续玩；断开至少让他知道并重连恢复。
- 每个 Connection 的写出由该连接内的单一调度器串行执行；不同 Connection 之间仍并发写出，不共享调度器。请求工作指从输入准入到结果写出的整段（读输入 → 准入 gate → 执行 → 写结果），是一个调度单元，不是仅写结果帧；MUST NOT 把请求执行留在原链、只把结果帧交给新队列，那会产生两个写者，违背单写者前提。请求工作 MUST 优先于事件：队列中有就绪的请求工作时，先处理该请求工作；事件的放行间隔由实现决定，但 MUST 保证事件的等待被有界数量的请求工作限制。结果帧 MAY 越过尚未写出的事件帧，事件帧之间仍保持发射序（范围见本节“投递语义”）。准入拒绝与关闭拒绝的响应帧不经过该调度器：它们不是请求工作，也不承载事件（§5.4 允许这类帧越过已接受输入）。
- 请求优先 MUST NOT 使事件无限期饥饿：事件的等待 MUST 被有界数量的请求工作限制。放行间隔是实现常量，不构成契约；请求优先只决定两者同时就绪时的顺序，公平规则只承诺有界等待，不承诺固定的请求与事件比例。
- 残余边界：已经交给传输的那一帧事件无法被后续结果抢占；慢客户端上正在写的事件帧会让下一个结果多等一帧的写出时间。这是单写者的固有代价，不是保证。
- 连接层开始关闭后 MUST 停止接收新事件；每一处事件缓冲内尚未写出的事件，以及 Session 泵已取出、尚未交给任何连接队列的在手帧，MUST 丢弃并记录一条汇总日志（`EVENT_BACKLOG_DISCARDED`，`reason: 'shutdown'`，字段见 §7.1）。关闭只排空请求工作，不为事件消耗关闭预算。已经开始写出的事件帧不属于队列内待写项，MUST NOT 被抢占；它仍受写出上界约束，排在其后的请求最多等待该帧的写出上界。汇总日志的 `count`、`frames`、`bytes` 是被丢弃的帧数与字节数，`connections` 是持有被丢弃帧的 Connection 事件队列数；Session 待投递队列与在手帧不计入 `connections`。依据：事件是 best-effort，关闭不为不可靠通道延迟资源释放。在手帧只存在于 takeover 重投递的交接窗口，平时不构成额外缓冲。
- 全部缓冲的字节总数达到或超过 `eventBacklogWarnBytes`（默认值见选项定义）时记一条 warn（`EVENT_BACKLOG_HIGH`，字段见 §7.1），同一段积压只报一次：只有聚合总量回落到零才重新武装。阈值附近的一次交接会让总量短暂归零，所以必须等稳定回落到零才重新武装。这是观测，MUST NOT 据此拒绝、丢弃或延迟任何事件：全局硬界会让一个吵闹的发射方导致所有连接丢事件，把局部慢读放大成全局事件丢失。部署方按容器内存设置该阈值，使其早于 OOM 触发；框架不承诺该阈值之内一定不 OOM，它只是让容量不足可观测。

安全边界：框架不对事件做授权。Handler 可以向任意 Session 声明事件；房间成员资格、可见性与内容过滤属于业务。

## 4. 存储契约

### 4.1 接口边界

框架包 MUST NOT 包含任何存储后端实现，MUST NOT import 具体后端包。存储接口（读、写、事务开启/提交/回滚）与能力声明类型的签名稳定；能力差异通过能力声明表达，而不是通过接口分叉或运行时嗅探。新增后端不得要求改动框架核心。理由：后端与具体驱动、方言、连接模型强绑定，发版节奏与框架不同；放在同一个包里，任一方升级都会拖累另一方。

### 4.2 能力声明

后端 MUST 导出能力声明。字段语义如下，类型签名以 `StorageCapabilities` 类型定义为准：

| 字段                       | 类型                            | 语义                                                                |
| -------------------------- | ------------------------------- | ------------------------------------------------------------------- |
| `transactions`             | `'full' \| 'limited' \| 'none'` | full：多实体原子；limited：仅单实体/单键原子；none：无回滚          |
| `concurrency`              | `'single' \| 'pooled'`          | single：单连接串行；pooled：连接池并行                              |
| `snapshotRead`             | `boolean`                       | 事务内读是否为一致快照                                              |
| `uniqueConstraint`         | `boolean`                       | 是否提供原子唯一约束（检查与写入不可分割）                          |
| `maxTransactionDurationMs` | `number`                        | 后端自身的事务时长上限；框架不得超过它；无上限时显式声明 `Infinity` |

所有字段都是必填。缺任何一个字段都会让框架的降级判断读到 undefined；无法提供的值 MUST 显式声明为最弱的一档（如 `transactions: 'none'`、`maxTransactionDurationMs: Infinity`），而不是省略。发布前能力名可删除或改名，按破坏性变更处理；发布后只增不删。新增能力字段同样属于破坏性变更，MUST 同步升级所有后端（§8）。框架 MUST NOT 假设后端具备未声明的能力。

### 4.3 降级规则

框架按声明选择路径，并把降级事实记录为告警级日志。对每个降级维度 MUST 记录一条 `warn`，在后端进程启动时各记录一次，不按请求刷屏；该日志行 MUST 包含 `capability`（能力名）、`declared`（后端声明值）、`consequence`（要求业务承担的后果）。全强度声明 MUST NOT 产生告警。同一次启动另有一条 info 记录完整能力声明；告警文案与日志行的产出顺序不是契约（§7.1）。

`transactions: 'none'` 的写入中止由 Dispatcher 执行：首次写入失败后，同一请求的后续写入在框架层拒绝、不再进入后端；读取不受此限，已写入部分不撤销。

| 声明                    | 框架行为                                                                         | 业务需要承担的                                      |
| ----------------------- | -------------------------------------------------------------------------------- | --------------------------------------------------- |
| transactions: full      | 提供 §3.3 的全部语义                                                             | 无需额外处理                                        |
| transactions: limited   | 事务只覆盖单实体，跨实体写入不保证原子                                           | 需要多实体原子性的 Extension 必须声明需求并拒绝降级 |
| transactions: none      | 无回滚；失败时中止后续写入，已写入的部分保留                                     | Extension 必须自行实现补偿或幂等                    |
| concurrency: pooled     | 框架层并发调度可落到存储层并行执行                                               | 无需额外处理                                        |
| concurrency: single     | 框架层并发调度不转化为存储层并发，多个事务在后端内部串行执行，吞吐受后端串行限制 | 不得依赖并发写入提升吞吐；长事务会直接阻塞其他请求  |
| snapshotRead: false     | 框架不承诺事务内重复读一致                                                       | Extension 自行处理读到变化数据的情况                |
| uniqueConstraint: false | 框架不提供“检查再写入”的原子性                                                   | 唯一性由单写者或业务侧幂等键保证                    |

Extension 对存储能力的依赖 MUST 显式声明。启动时若后端能力不满足某个 Extension 声明的最低要求，框架 MUST 拒绝注册该 Extension（fail fast），不影响其他 Extension。被拒绝的 Extension 不执行注册，其 Handler 全部不存在；声明 `required` 的 Extension 被拒绝时整个业务进程拒绝启动。依赖声明的粒度是“最低要求”，不是“完整能力清单”：例如银行类 Extension 要求 `transactions: full` 与 `uniqueConstraint: true`；只读查询类 Extension 可以不声明任何存储能力要求，从而在任何后端上运行。

理由：静默降级会在最需要原子性的时刻（崩溃、并发）暴露问题，而那时已经无法回头修改设计。

### 4.4 单连接后端

`concurrency: single` 的后端 MUST 在内部串行化事务，否则一方回滚会吞掉另一方已写入的数据。这是正确性要求而非性能优化；框架 MUST NOT 感知该排队。

### 4.5 后端职责

一个后端至少实现基础读写与事务开启/提交/回滚。能力声明决定这些操作的实际保证强度，不决定它们是否存在；后端 MUST 如实声明，不得为了通过启动校验而虚报。

### 4.6 写者

业务进程是存储的唯一写者，连接层进程 MUST NOT 直接写存储。若将来引入多写者，MUST 显式声明并给出冲突解决策略，不得以“后端能处理”为由默认放开。

### 4.7 不做 ORM

存储契约是数据访问的最小面，不是对象映射层。框架不提供实体关系图、级联、延迟加载或自动迁移。理由：MUD 的状态结构由业务演化，框架级映射层会变成阻碍；后端只需能存取值、能开事务。

## 5. 传输契约

### 5.1 接口边界

框架核心 MUST NOT 包含任何具体传输实现，MUST NOT import 具体传输包。`Transport` 接口的签名稳定。`connector-ws` 等实现是独立包，独立发版、独立测试。新增传输只需实现接口，不得要求改动框架核心。理由：传输与具体协议、帧格式、握手强绑定，发版节奏与框架不同；放在同一个包里，任一方升级都会拖累另一方。

### 5.2 传输职责边界

Transport 只负责四件事：接受连接、交付入站数据、写出发站数据、关闭。MUST NOT 承担：Session 路由、输入缓冲、重放、访问检查。理由：这些职责依赖 Session 与 Request 语义，属于框架契约；下沉到传输会让每个传输实现各自重写一遍，且无法保证跨传输一致。

Transport MUST 对每个连接的发站缓冲设上限。缓冲耗尽时 MUST 显式失败并终止该连接，不得让未写出的数据无界积压，也不得静默丢弃。单条发送的超时只约束“已接受的帧迟迟写不出去”，MUST NOT 用它代替容量上限：慢客户端可以在超时前持续制造积压。传输实现 MAY 在出站缓冲耗尽时以 close code `1013`（Try Again Later）关闭该连接；这与 §6.1 的准入拒绝使用同一码值，但触发条件不同（已准入连接的服务不可用，而非准入拒绝），传输实现可以映射为本协议的等价语义。

Transport MAY 暴露自己的单帧发送上界（`Transport.sendTimeoutMs`）。暴露时框架用它给所有下行帧的写出设界——事件帧与响应帧一样——保证同一 Session 的串行位不会早于传输自身失败而释放（§3.8），也让一个不读的客户端无法永久占住连接的单写者。框架的界在该值上加一段固定余量派生（§3.8），在 Bun `setTimeout` 可接受的最大延时（2^31−1 ms）以下严格宽于传输的界，从而让传输自己的超时判定成为可观测的那个；触顶时以封顶值为准。该字段可选，未暴露或值不可用（例如 `Infinity`）时框架按自己的配置与默认值处理。

`TransportConnection.id` 在连接层实例的生命周期内 MUST 唯一：连接断开后，同一 id MUST NOT 被新连接复用。连接层以该 id 维护活跃连接、Session 路由、请求入口查找与事件投递；复用会让迟到入站消息落到替换连接，或让本应发给旧连接的事件误投到替换连接。需要重连语义时，传输 MUST 铸造新 id（默认 WS 传输的 UUID 满足此约束）。连接层按连接对象身份忽略迟到的旧断线事件，但按 id 投递的事件无法区分新旧，因此唯一性由传输保证。

职责拆分：Session 路由、输入缓冲与重放在连接层进程（§6.1）；访问检查点与拒绝语义（`AUTH_REQUIRED` / `FORBIDDEN`、检查顺序）随 Handler 在业务进程执行（§3.7）；认证与授权策略、登录流程属于消费方。

TLS 是传输/部署边界，不是认证机制，也不属于 core 的职责：core 不终止 TLS，也不承载 TLS 配置，只处理传输已经解出的 payload。部署方 MUST 在不可信网络前终止 TLS——由反向代理前置终止，或由具体传输实现自行终止；框架不承诺明文监听可以安全地暴露在不可信网络上。TLS 终止不替代消费方的认证与授权策略（§3.7），也不改变 §5.4 的 Envelope 线格式。

### 5.3 独立维护边界

传输实现升级不触发框架核心发版；框架只保证 `Transport` 接口稳定，接口变更属破坏性变更（§8.2）。框架核心 MUST NOT 包含或 import 具体传输实现（§5.1）。

### 5.4 客户端线格式

客户端与连接层之间的线格式属于框架契约，以 `Codec` 接口与 `Envelope`、`ResponseEnvelope` 类型为准。字段名是契约；默认的 `JsonCodec` 以 JSON 编码，替换 `Codec` 即替换编码方式。

请求是 `Envelope`：`{id, method, payload}`。`id` 是客户端提供的关联标识，响应原样回填；`method` 命名一个 Handler；`payload` 交给 Handler 解释。响应是 `ResponseEnvelope`：`{id} & ({ok: true, result} | {ok: false, error: {code, message}})`。`payload` 与 `result` 对框架不透明——框架不解析、不改写。客户端的 `id` 不是 §7.3 的请求标识：后者由框架在入口分配，不出现在客户端线格式中。

`ok` 是必需的判别字段，不能由可选字段的组合代替：`{"id":"x","ok":true}` 让“成功且无返回值”成为显式判别分支；JSON 编码会丢弃值为 `undefined` 的字段，省略 `ok` 后该响应与缺少 `ok` 的非法帧无法区分。失败分支的 `error` 只有 `code` 与 `message`，`code` 的取值见 §7.2，`message` 是供人阅读的说明而非稳定契约。

默认 `JsonCodec` 的入站校验：解码结果必须是对象，`id` 与 `method` 必须是非空字符串；其余校验细节由 `Codec` 实现决定。

连接层正常接受输入时，入站帧解码失败不构成 Request，但 MUST 占用有界输入队列的席位。取得席位的坏帧按连接顺序处理：记录一条 warn（`code` 为 `INVALID_ENVELOPE`，字段含 `connectionId`、`rawLength`、`reason`），并以 `id` 为空字符串的失败响应回 `INVALID_ENVELOPE`，不从坏帧中提取关联标识，也不因一帧坏数据终止连接。队列已满时，坏帧立即以 `id` 为空字符串的 `FRAMEWORK_QUEUE_FULL` 拒绝，不追加排队任务；这类拒绝响应可以越过已接受输入，只有已取得席位的输入遵守排队顺序。

关闭流程中（连接层状态为 stopping 或 stopped）新到达、尚未进入 Input Queue 的输入一律以 `FRAMEWORK_SHUTDOWN` 拒绝，包括无法解码的帧，不进入请求处理；已接受的输入按 §6.3 排空或显式结算，不保证全部执行。能解码的帧回填其 `id`，无法解码的回 `id` 为空字符串。关闭期间未跟踪连接上的输入同样收到 `FRAMEWORK_SHUTDOWN`；未跟踪连接上、且不在关闭流程中的输入没有响应：连接层没有该连接的活跃记录，不为它解码或回写。

`result` 无法被当前 `Codec` 编码时，框架用同一 `Codec` 编码一条 `INTERNAL_ERROR` 失败响应作为兜底；若该兜底仍无法编码，框架只能记录 `TRANSPORT_SEND_FAILED`，不承诺发出响应。

响应帧的写出 MUST 有界，上界与事件帧同源（§3.8、§5.2）：超过上界时记录 `TRANSPORT_SEND_FAILED` 并继续，不改变请求终态，也不承诺该响应送达。这个界覆盖所有响应帧，包括不经过连接调度器的准入拒绝（`FRAMEWORK_QUEUE_FULL`）与关闭拒绝（`FRAMEWORK_SHUTDOWN`）帧：它们直接在输入处理路径上写出，阻塞会卡住该连接的后续输入。

下行帧有两类，响应帧的线格式保持不变：响应是 `{id, ok, ...}`，不带 `kind`；事件帧是 `{kind:'event', eventId, event, payload}`，`kind` 是事件帧的判别字段。事件帧的 `eventId` 为 `${requestId}:${index}`，同一请求、同一发射顺序的重放得到相同值，客户端可用它去重；框架不保证事件不重复。客户端 MUST 忽略不认识的帧，MUST NOT 因一个事件帧终止连接。事件投递失败不影响任何响应帧。

## 6. 生命周期

### 6.1 进程关系

连接层进程与业务进程分离。连接层持有传输连接、Session 路由、Input Queue 与未确认输入缓冲；业务进程持有 Extension、Handler 执行与存储访问。

分离的理由不是架构美观，而是崩溃恢复：业务进程崩溃后可以重启并重放，连接层保住玩家不断线。Session 的业务状态归业务进程，连接层只保存路由信息与未确认输入；连接层重启会断开连接，但不会丢失已提交的业务状态。

连接层对同时活跃的连接数设准入上限（见 `ConnectionLayerOptions.maxConnections`）。达到上限时，新连接 MUST 被关闭且不进入请求处理；关闭时传给传输的关闭码 MUST 为 `1013`（Try Again Later），非 WebSocket 传输可以忽略该值。连接断开后空出的名额 MUST 可供后续连接使用。该上限只约束准入，不是业务 Session 的生命周期限制。理由：绑定在 Handler 成功后才声明，连接层无法在执行前预检 Session 容量；活跃连接数是唯一能在副作用前拒绝的位置，而路由表大小不会超过连接数。

### 6.2 启动顺序

1. 存储后端初始化并自检能力声明。
2. 业务进程加载 Extension，按 §4.3 校验其声明的存储能力；业务进程启动所必需的能力不满足时拒绝启动业务进程。
3. 连接层开始接受连接。

连接层 MUST NOT 在业务进程就绪之前接受输入。理由：否则第一批玩家会收到“框架正在启动”这类本不该存在的错误。

就绪由两阶段握手表达（ADR-0007）：业务进程收到 `hello` 后先回 `prepared`，此时 MUST NOT 打开存储、注册 Extension 或执行 Handler；只有收到 `activate` 才执行第 1、2 步并安装请求处理器，然后发 `ready`。连接层在收到 `ready` 之后才开始接受连接。边界：`prepared` 只表示进程活着且等待激活，不表示可用；连接层 MUST NOT 向 prepared 的业务进程投递 Request。

产品约束（部署期间不接受可感知的秒级延迟上升、不接受玩家重连）记录在 ADR-0007；它不是协议时限或观测口径，`switchTo` 只承诺有界缓冲与连接不断开。

初次 `start()` 的整体等待受 `startTimeoutMs` 约束，期间的失败重试不延长总时限；自动恢复启动的每一代另有同值的独立握手时限，从该代启动到 `ready`。计时器 MUST 绑定该代，在该代 ready、丢失或关闭后失效，不得影响其他代或使终态执行器复活。切换的 prepared 与 ready 两个等待分别有界（ADR-0007）。

版本切换复用同一协议，顺序是契约：连接层先起新进程并等到 `prepared`，再停旧进程、等它 `closed` 且退出被观测到，然后才向新进程发 `activate` 并等 `ready`，最后把缓冲请求投给新进程。`activate` MUST NOT 在旧进程仍可能持有存储时发出（INV-002）；`closed`、kill 返回与退出等待超时都不能代替退出确认。切换期间连接层 MUST 继续按容量与 deadline 接受、缓冲请求，不得向即将退役的旧进程或尚未 `ready` 的新进程投递；新进程 `ready` 后按 §6.5 的顺序投递。

切换失败时，旧进程未被 drain 则 MUST 继续服务；已经退役则使用切换前的 spawn 恢复。回滚重启 MUST 等到旧进程与已 `activate` 的新进程退出均被观测到；无法确认时保持写者屏障并显式失败，不得以恢复可用性为由启动另一写者。切换成功提升后，连接层 MUST 采纳目标 spawn，供后续崩溃重启使用。`close()` 在 `activate` 之后中止切换时，MUST 对放弃的新进程执行有界的退出确认（等退出 → kill → 再等）；确认失败时关闭 MUST reject，而不是只记日志后成功返回。

`switchTo` 的失败是结构化的：rejection 是 `ServerSwitchError`，其 `details` 至少给出失败阶段（`preparing` / `retiring` / `activating`，与切换自身的阶段边界一一对应）、旧进程是否仍在服务、是否已回滚、替换进程是否已提升、是否有退出未确认的进程，以及 `close()` 是否已接管收尾。字段名与取值是契约；`message` 不是。理由：这三种失败留下的状态截然不同——旧进程仍在服务时可以安全重试，已回滚则重试会叠在正在恢复的进程上，`closeAborted` 时执行器正在关闭、重试会被拒绝——只有文本 message 无法让消费方区分。

### 6.3 关闭顺序

1. 连接层停止接受新输入。
2. 排空已接受输入，受连接层关闭预算约束。尚未开始的请求分两种情形，语义不同：
   - **已从队列取出、仍在 Session 屏障前等待**：abort 解除等待，请求以 `FRAMEWORK_SHUTDOWN` 结算并回执（连接仍在，回执送得出去），不绕过屏障继续执行。
   - **仍在连接调度器队列中、从未取出**：排空结束后 MUST 被显式丢弃，逐条记录 `REQUEST_DROPPED_AT_SHUTDOWN`（字段见 §7.1），不再执行也不回执——连接随即关闭，回执无处可送。

   已经开始执行的请求按原路径结算并回执。丢弃只发生在排空之后：排空预算内能开始的请求仍正常执行，不因关闭而被提前丢弃。

3. 执行器等待已接受工作，并关闭 Server 与存储，所有等待有界；超时或失败必须可观测，不得当作已确认资源释放。
4. 尽力关闭传输并传播其失败，即使执行器关闭已经失败也不得跳过。

所有关闭等待 MUST 有界。`ConnectionLayerOptions.shutdownTimeoutMs` 的显式值 MUST 是不小于 1 的有限数且不超过 Bun `setTimeout` 可接受的最大延时（2^31−1 ms）；超限时连接层构造抛错，不钳制——钳制会把最长等待变成 1ms，关闭时直接丢弃排队请求。`ConnectionLayer.stop()` MUST 在传输收尾后传播执行器与传输的关闭失败或超时：仅执行器失败时传播该失败，仅传输失败时直接拒绝，两者都失败时以 `AggregateError` 同时报告；不得以成功返回掩盖退出或监听未确认。`status` 变为 `stopped` 不等于资源已确认释放。重复调用 `stop()` MUST 返回同一关闭结果，不重新发起关闭。执行器有界返回不等于进程已经退出；旧写者退出未被观测时 MUST NOT 启动新的写者。完整外部停止预算必须覆盖连接层排空、执行器关闭与传输关闭三段，不能只使用 `IpcExecutor.closeBudgetMs`；公式与常量来源见 ADR-0004。

顺序的理由是先结束业务调度，再释放存储与传输；但关闭错误码不证明请求未执行或未提交，阶段边界见 §7.2。

### 6.4 崩溃恢复

承诺：连接保活 + 未确认输入重放。终态与确认的区别见 §2；重放缓冲的移除条件是确认，不是终态。

恢复受唯一写者屏障约束：只有 `SpawnedServer` 报告退出 `observed: true` 才能解除旧写者屏障。退出 Promise 仍 pending 而等待超时时，框架 MUST 保留引用，迟到的确认可在执行器仍可恢复时继续一次恢复；`close()` 或其它终态之后 MUST NOT 被迟到事件重新启动。若一次性退出结果是 `observed: false`，框架无法据此确认进程释放了存储，MUST 保持屏障，不承诺自动恢复；有界关闭仍无法确认退出时 MUST 失败。恢复可用性不能优先于 INV-002，请求仍受各自 deadline 约束。关闭未能确认退出时，健康态 MUST 保持 `blocked`（§6.6）。

子进程诊断：`ServerExitInfo` 有一个可选字段 `stderrTail`，携带 spawn 被要求捕获时子进程 stderr 的末尾若干行；它是诊断信息，不参与退出判定——`observed` 仍是解除屏障的唯一依据，尾部是否为空或缺失不影响任何恢复、关闭或切换语义。切换失败时框架 MUST 在有诊断可用时把它带进 rejection，使消费方能读到“Server 为什么起不来”，而不是只能从 Portal 控制台看到。捕获开关与排空代价见 `docs/INTEGRATION.md`。

语义是 at-least-once：同一个 Request 可能被执行多次。框架 MUST NOT 声称 exactly-once。幂等由业务负责：需要“只生效一次”的效果（扣钱、发物品、唯一命名），MUST 由 Extension 用业务幂等键实现，不能依赖框架。理由：at-least-once 是“连接保活”承诺的直接代价——只要允许重放，重复执行就无法在框架层排除。

### 6.5 重放的顺序约束

重放 MUST 保持原顺序：同一 Connection 上未确认的 Request 按接收顺序重放。理由：重放若乱序，等于在恢复路径上重新引入 §3.1 已经排除的问题，而且发生在最不稳定的时刻。

重放不是无限重试。同一 Request 连续失败到上限后 MUST 进入终态并记录错误，不得无限重放——那会把一个坏输入变成对业务进程的持续攻击。

### 6.6 就绪快照

`ConnectionLayer.health()` MUST 返回同步快照，可在 `start()` 之前与 `stop()` 过程中调用，不得产生副作用。`ready` 仅在连接层已启动且执行器就绪时为 true；连接层已启动但执行器未就绪时 `state` 为 `degraded`。`listening` 只表示传输仍在接受连接，不表示业务可用；`connections` 是当前已准入的连接数。传输关闭未确认时 `listening` 不可信：`status=stopped` 会让 `listening=false`，但监听可能仍在运行；此时以 `stop()` 的失败（或超时）为准，不能把快照当作“监听已释放”的证据。

执行器通过 `RequestExecutor.health()` 自报状态：`ready` 只在当前可以立即执行、无需等待恢复时为 true，启动中、重启中、等待新一代 `ready`、已关闭与阻塞都为 false。`blocked` 表示旧写者退出未确认，恢复屏障未解除（INV-002）；此时 MUST NOT 以恢复可用性为由启动新的写者。关闭失败未能确认退出时同样保持 `blocked`。

快照是建议性的，可能在返回后立即过期，MUST NOT 作为每个请求的准入 gate——请求仍受 §3.5 队列上限、§6.2 启动顺序与各自 deadline 约束。包内不提供传输层健康端点：如何把快照映射为 HTTP 探针由应用决定（见 INTEGRATION.md）。

快照包含聚合字段 `eventBacklog: { connections, frames, bytes, dropped }`：`connections` 只统计事件队列非空的 Connection；Session 待投递队列在事件解析出 holder 之前不归属于任何 Connection，只体现在 `frames` 与 `bytes` 中。`frames` 与 `bytes` 是当前全部事件缓冲（每 Connection 的事件队列与每 Session 的待投递队列）的在途总量，含正在写出、尚未结算的那一帧；`dropped` 统计因事件缓冲达到上界而被断开的 Connection 数，是自本连接层启动以来的累计值（不是瞬时值），不含关闭时的批量丢弃——后者走 `EVENT_BACKLOG_DISCARDED` 汇总日志，不进入该计数。字段名 `dropped` 保留（探针契约），语义是断开数而不是事件数（§3.8）。理由：探针读 `dropped` 是为了判断运行期是否在持续出现积压断开（背压或滥用）；把关闭期的正常丢弃混进来，会让每次滚动重启都出现一次计数跳变，形成误报。快照不提供 per-connection 深度，单连接诊断走日志（§7.1）。

## 7. 可观测性

### 7.1 结构化日志

框架日志 MUST 是结构化的（字段而非拼接字符串）。字段名是契约，文案与日志行的产出顺序不是契约。每个 Request 的日志 SHOULD 至少携带请求标识、Session ID、Connection ID、Extension 名与耗时；失败时带 `code`，成功由日志消息表达，不承诺 `result` 字段。日志级别按语义使用：告警级用于能力降级、重放、队列拒绝这类“能继续运行但需要人知道”的事件；错误级用于请求失败；不得用错误级记录预期内的业务拒绝，包括 `AUTH_REQUIRED` 与 `FORBIDDEN`（§3.7、§7.2）。

注入的日志实现抛错 MUST NOT 改变框架控制流。日志实现失败时框架不保证 `child()` 的绑定字段完整：`safeLogger` 的 `child` 抛错会回退到父包装。

`RequestContext.extension` 由 handler 的注册元数据填充。通过 `ExtensionRegistrar` 注册的 handler 自动带上所属 Extension 名，请求日志的 bindings 仅在存在该名时写入 `extension` 字段；不属于任何 Extension 的 handler 不带该字段。

边界：`extension` 字段只有 Server 侧在 handler 注册元数据可知时填充。Portal 不持有 handler 注册信息，其请求级日志（例如拒绝、队列满、超时、取消、重放）无法获知 Extension，不能按该字段过滤 Portal 日志；这些日志与 Server 侧日志依靠请求标识与 Connection ID 关联。

边界：请求结算之后产生的日志（如 `late result discarded`）依靠结果消息恢复 Session ID。结果消息回显请求进入框架时的入口 Session ID（`requestSessionId`，成功与失败结果都携带；入口无 Session 时不带），迟到结果用它填 `sessionId`；结果消息的 `sessionChange` 仍只表示 handler 在本次请求内声明的 Session 变更（bind 携带新 Session，unbind 表示解绑）。结果未带 `requestSessionId` 时，已结算的日志退回用稳定的请求标识（§7.3）与同一请求的其它日志行关联。

结果/完成类日志（`request completed`、`result received`、失败日志）的 `sessionId` 表示请求入口 Session（与 `RequestIdentity` 同义），不会因 Handler 绑定而被改写；入口无 Session 时不写。成功请求声明了 Session 变更时写 `sessionChange`，值为 `SessionChange` 对象（`{type:'bind',sessionId}` 或 `{type:'unbind'}`）；没有变更不写。字段名与语义都是契约。路由事件行（`session bound`、`session rebound to a new connection`）的 `sessionId` 表示本次被建立或接管的路由 id，与请求入口 Session 无关。

Server 事件的诊断日志（ADR-0008）分逐事件与批量两类。逐事件日志（`EVENT_SEND_FAILED`）带 `eventId`、`event`、`reason` 与目标字段。Server 侧的 `EVENT_SEND_FAILED` 与请求入口身份写在同一行，目标字段用 `target` 前缀与入口身份区分：`targetKind` 取 `'connection'` 或 `'session'`，并相应带 `targetConnectionId` 或 `targetSessionId`；入口身份字段（`requestId`、`connectionId`、`clientRequestId`、`sessionId`）语义不变，`sessionId` 始终是请求入口 Session，不是投递目标。批量日志 `EVENTS_DROPPED` 是该请求的全部事件随未发出的结果一起被跳过时的单行记录，只有 `count`、`reason` 与请求入口身份，没有单一的 `eventId` 或目标字段。Portal 侧的 `EVENT_DROPPED`、`EVENT_LISTENER_FAILED` 带目标字段，沿用目标自身的 `connectionId` 或 `sessionId`，并同样带 `targetKind`；`EVENT_DROPPED` 可以代表一批同类丢弃，见下段。`code` 取值：`EVENT_DROPPED`（目标缺失、连接已关闭，或没有事件监听者）、`EVENT_BACKLOG_DISCONNECTED`（事件缓冲达到上界，断开该连接，§3.8）、`EVENT_SEND_FAILED`（Server 侧单条事件 IPC 发送失败）、`EVENTS_DROPPED`（结果未发出，该请求的全部事件被跳过）、`EVENT_LISTENER_FAILED`（事件监听者抛错）、`EVENT_BACKLOG_DISCARDED`（关闭时丢弃未写出的事件，单行汇总）；Portal 侧结果无法编码时记录 `INTERNAL_ERROR`（`stage: 'encode'`），并用同一 `Codec` 编码一条兜底失败响应；兜底仍无法编码时记录 `TRANSPORT_SEND_FAILED`（`stage: 'encode'`）。写出超时或被拒时记录 `TRANSPORT_SEND_FAILED`（`stage: 'send'`）。这些是仅在日志中出现的诊断字段，不发往客户端；事件投递失败 MUST NOT 改变请求的日志结论或终态。

`EVENT_LISTENER_FAILED` 的“事件监听者”指通过公开 API `RequestExecutor.onEvent` 注册的 sink：注册返回退订函数；一次事件按注册顺序同步调用所有 sink；单个 sink 抛错只记 `EVENT_LISTENER_FAILED`，不影响其他 sink，也不改变任何请求的终态；执行器关闭后注册为 no-op；没有可用 sink 时事件丢弃并记 `EVENT_DROPPED`。`ConnectionLayer` 在启动时注册一个 sink，`onEvent` 保持为公开 API。

事件缓冲达到任一处上界时为 `EVENT_BACKLOG_DISCONNECTED`（`reason: 'event buffer reached its bound'`）：字段含 `connectionId`（被断开的 Connection）、`buffer`（`'connection'` 或 `'session'`，指明哪一处缓冲溢出）、`closeCode`（传给传输的关闭码 `1013`）、`eventId` 与 `event`（触发断开的那条事件，用于回答「哪条事件压垮了队列」）、`queuedEvents`、`queuedEventBytes`、`maxQueuedEvents`、`maxQueuedEventBytes`（溢出时的观测值）；`sessionId` 在缓冲归属一个 Session 时带上：Session 待投递队列溢出必带（溢出队列属于该 Session），Session 目标投递引发的 Connection 队列溢出也带（标识被投递的 Session，与 `connectionId` 一起给出「哪个 Session 的投递压垮了哪条连接」）。connection 目标投递不带，因为那条事件本就没有 Session。这条日志每个被断开的 Connection 一行，不聚合：一次溢出只断开一次，不构成慢读者那种逐事件流。

Portal 侧的 `EVENT_DROPPED` 按（`reason`，目标 id）聚合，同一批的后续丢弃合并进下一行，不是每条丢弃一行。理由：慢读者使每条新事件都成为一次丢弃，逐条记录会把诊断通道变成第二条无界流——正是事件上界要卸掉的压力；注入同步写文件的 logger 时还会直接阻塞事件循环。聚合保证：该组合的首条丢弃立即记录，之后按对数增长记录，静默达到 `QUIET_RESET_MS` 后的首条也立即记录；低速率的目标不会因累计值变大而长期静默。算法见 ADR-0008。

聚合行 MUST 保留 `code`、`reason`、目标字段（`connectionId` 或 `sessionId` 与 `targetKind`）以及 `count`（本行代表的丢弃条数）与 `droppedTotal`（该组合的累计丢弃数）。`eventId` 与 `event` 取本批首条丢弃的事件，用于回答「丢的是哪条」；聚合行不声称本批只有该事件，`count > 1` 即表示它是首条。`EVENT_BACKLOG_DISCONNECTED` 只描述断开本身：被断开连接上尚未写出的帧仍逐条记 `EVENT_DROPPED`，经本条聚合，所以一次溢出只产生一行断开日志，加有界数量的聚合丢弃行。这些丢弃的 `reason` 与客户端主动断开不同（`the connection was closed after an event backlog overflow` 对 `the target connection closed before the event was written`），运维据此区分「客户端走了」与「框架断开了它」。聚合只影响日志：`eventBacklog.dropped` 按断开计数（§6.6），探针读到的不是聚合值。聚合状态属于本连接层实例，重启即清零；同时活跃的（`reason`，目标 id）组合数有上界 `MAX_BUCKETS`。表满时先淘汰静默达到 `QUIET_RESET_MS` 的组合，仍满则淘汰最久未再丢弃的组合，被淘汰的组合先 flush 再删除；新组合与被淘汰组合的后续丢弃都从新桶重新计数，其首条立即记录，之后继续按对数增长。

关闭时丢弃未写出事件的汇总日志为 `EVENT_BACKLOG_DISCARDED`，`reason` 取 `'shutdown'`，字段含 `count`、`connections`、`frames`、`bytes`（§3.8）。

聚合事件缓冲达到或超过告警阈值时为 `EVENT_BACKLOG_HIGH`，字段含 `bytes`（当前全部缓冲的字节总数）、`frames`、`activeConnections`（当前已准入连接数，与 §6.6 快照的 `connections` 不同：后者只数持有帧的队列）、`warnBytes`（生效的阈值）。该行只记录观测，不改变任何事件的投递结果（§3.8）。

关闭时丢弃尚未开始的请求为逐条日志 `REQUEST_DROPPED_AT_SHUTDOWN`（§6.3 第 2 步），`reason` 指明排空预算内未开始，字段含 `requestId`、`method`、`clientRequestId`（无则省略）、`connectionId`。该 `code` 用于「从未从调度器队列取出的请求」；已取出但在 Session 屏障前被 abort 的请求走 `FRAMEWORK_SHUTDOWN`（§6.3 第 2 步、§7.2）。`TRANSPORT_CLOSED` 表示连接已不在，不用于关闭预算耗尽。边界：排空等待超时后 `doStop` 才清空连接表，此时仍停在 Session 屏障前的请求会看到连接不匹配，落 `TRANSPORT_CLOSED` 而不是 `FRAMEWORK_SHUTDOWN`；它同样不执行。

### 7.2 错误码

错误码 MUST 可枚举。发布前错误码可删除或改名，按破坏性变更处理；发布后只增不删。语义变更 MUST 分配新码，不得复用旧码表示新含义。

**wire 码** 随 `error.code` 发给客户端（`ok: false` 时），属于对外契约，MUST 稳定可枚举：

| 码                           | 含义                                       |
| ---------------------------- | ------------------------------------------ |
| `FRAMEWORK_TIMEOUT`          | 请求超时                                   |
| `FRAMEWORK_QUEUE_FULL`       | 输入队列或 Portal 未确认缓冲拒绝（§3.5）   |
| `FRAMEWORK_SHUTDOWN`         | 因关闭未取得成功确认，不保证未执行或未提交 |
| `FRAMEWORK_REPLAY_EXHAUSTED` | 重放次数用尽，请求进入终态（§6.5）         |
| `TRANSPORT_CLOSED`           | 传输在请求完成前关闭                       |
| `TRANSPORT_SEND_FAILED`      | 结果无法写回传输                           |
| `INVALID_ENVELOPE`           | 入站帧无法解码                             |
| `UNKNOWN_METHOD`             | 没有该方法的 Handler                       |
| `INTERNAL_ERROR`             | 框架内部失败，含未声明的业务错误           |
| `AUTH_REQUIRED`              | Handler 要求 Session，但请求未携带（§3.7） |
| `FORBIDDEN`                  | 注入的访问策略拒绝了请求（§3.7）           |

`AUTH_REQUIRED` 与 `FORBIDDEN` 是预期内的拒绝，不是框架失败：它们发生在存储事务与 Handler 之前，日志级别为 warn（§7.1），MUST NOT 按错误级记录。它们证明本次请求没有打开事务、执行 Handler 或改变 Session 路由。

`FRAMEWORK_SHUTDOWN` 必须区分发生阶段：关闭时拒绝的新输入，以及仍在 Session 屏障前、尚未调用 executor 就被关闭结算的请求，确定没有被调度执行；可能已投递给 Server 却未取得成功确认的请求，则可能已经执行或提交。此码本身不携带阶段信息，MUST NOT 仅凭它判断可无幂等保护地重试；恢复与重试仍须遵守 §6.4 的业务幂等边界。

`TRANSPORT_SEND_FAILED` 的触发条件是“写不出去”，因此它结构上不可能送达客户端，只能作为日志字段被观察到。`TRANSPORT_CLOSED` 只作为请求终态的原因出现：请求因断线结束时连接已不在，没有可回写的通道，因此不承诺送达。两码仍是契约——它们回答“请求为什么结束”，删掉会让终态失去可枚举的说法。

**仅日志诊断字段** 出现在日志的 `code` 字段里，用于定位，不发给客户端，也不构成 wire 契约：`CANCEL_LOST_TO_COMMIT`、`IPC_CHANNEL_CLOSED`、`SERVER_STARTUP_FAILED`、`DUPLICATE_REQUEST`、`INVALID_IPC_MESSAGE`、`EVENT_DROPPED`、`EVENT_SEND_FAILED`、`EVENTS_DROPPED`、`EVENT_LISTENER_FAILED`、`EVENT_BACKLOG_DISCONNECTED`、`EVENT_BACKLOG_DISCARDED`、`EVENT_BACKLOG_HIGH`、`REQUEST_DROPPED_AT_SHUTDOWN` 等。日志里的 `code` 只是一个字段名，取值不受本节约束。`EVENT_DROPPED`、`EVENT_BACKLOG_DISCONNECTED`、`EVENT_BACKLOG_DISCARDED` 与 `REQUEST_DROPPED_AT_SHUTDOWN` 的 `reason` 与字段见 §7.1。

`FRAMEWORK_` 前缀只是命名习惯，不是分类依据：框架码由显式集合 `isFrameworkCode` 判定，不按前缀。业务码可以合法以 `FRAMEWORK_` 开头。无前缀的框架码完整列出：`TRANSPORT_CLOSED`、`TRANSPORT_SEND_FAILED`、`INVALID_ENVELOPE`、`UNKNOWN_METHOD`、`INTERNAL_ERROR`、`AUTH_REQUIRED`、`FORBIDDEN`。发布后错误码只增不删；改名等于换码。

业务错误码由 Extension 用 `BusinessError` 显式定义，框架原样透传，不改写、不吞掉。只有 `BusinessError` 的 `code` 与框架自身错误码进入 wire：驱动或运行时的偶然 `code`（如 `ENOENT`、`SQLITE_CONSTRAINT_UNIQUE`）不透传，统一降级为 `INTERNAL_ERROR`（INV-014）。

### 7.3 请求标识与关联

每个 Request 在进入框架时分配请求标识，并在整个生命周期内保持不变——包括跨进程投递与重放。理由：一次重放会产生多条日志，分布在两个进程；若重放时换新标识，就无法回答“这条日志对应玩家哪一次输入”。重放时 SHOULD 额外记录重放次数与原始接收时间（绝对时间戳），用于识别“同一个请求反复失败”的情况，并让分布在两个进程的日志能按原始接收时刻对齐。原始接收时间指连接层把该请求交给执行器的时刻，不是连接层收到帧的时刻：请求可能先在调度器队列与 Session 屏障前等待。Portal 侧的请求标识由 `ConnectionLayerOptions.createRequestId` 生成，默认 `randomUUID`；该标识随请求跨进程投递，执行器沿用而不重新生成。

## 8. 变更规则

### 8.1 代码与文档冲突

代码是唯一真相。代码与本文冲突时以代码为准，本文 MUST 修正。

唯一例外是 INVARIANTS：那里写的是产品承诺，违反它是 bug，不是文档过期。判定属于“代码错了”还是“承诺变了”，由修改者显式给出结论，不允许默认按代码改文档。SPEC 与 INVARIANTS 冲突时以 INVARIANTS 为准。

### 8.2 改 SPEC 需要什么

- 改动承诺（P1–P5）或 INVARIANTS：属于重大变更，MUST 说明改了什么、为什么原承诺不再成立、受影响的代码与迁移方式。
- 改动边界或术语：MUST 同步修改代码标识符、类型定义与两份文档，不留同义词。
- 改动存储能力：发布前能力名可删除或改名，按破坏性变更处理；发布后只增不删。
- 改动 `Transport` 接口签名按重大变更处理，MUST 同步升级所有传输实现。
- 纯补充说明：可直接改，不构成契约变更。

### 8.3 文档不写什么

本文不写模块职责表、目录结构、测试覆盖与实现步骤。这些随代码变化，写进本文只会过期。本文只写决策、边界与理由。契约只存在于本文与 `docs/INVARIANTS.md`；仓库可以另有明确标注为非契约的操作文档（如 `docs/INTEGRATION.md`）：它们写可复制的代码、装配步骤与部署检查清单，随实现变化，不构成承诺；与本文冲突时以本文为准。
