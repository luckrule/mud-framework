# SPEC

本文档定义框架的对外契约。代码是唯一真相；这里只写代码表达不了的部分——为什么这样选、边界在哪里。

强制程度用 MUST / MUST NOT / SHOULD 表达。未标注的段落是实现说明或理由，不构成契约。

## 1. 定位与承诺

### 1.1 定位

通用 MUD 游戏框架，运行于 Bun + TypeScript。

框架的设计目标是提供让 MUD 业务能安全跑起来的骨架：连接的接入与有序化、请求的事务边界、存储后端的能力协商、业务进程的生命周期与崩溃恢复、可观测的日志与错误码。游戏规则、命令解析、世界模型属于 Extension，不属于框架。上述核心能力已实现；传输能力协商已从契约中移除（接口边界见 §5.1，传输能力协商的移除理由见 ADR-0005），Extension 生命周期钩子已从 Extension 契约中移除——它既没有语义定义，也没有消费者。

读者是 AI 协作者与未来的维护者。因此本文优先写决策与边界，不写机制教程；机制细节以代码与类型定义为准。

### 1.2 承诺

以下五条是框架对使用者的硬承诺，每条附一句边界，说明承诺到哪里为止。

**P1 传输无关。**
Transport（传输层）可整体替换，框架不绑定 WebSocket、TCP 或任何具体协议。
边界：框架规定请求的边界与顺序，不规定 Transport 的内部编码、分帧与握手方式；连接层对客户端的线格式属于框架契约，见 §5.4。新增传输只需实现传输接口，不得要求改动框架核心。

**P2 异步执行模型。**
Handler 可以 await，框架的调度建立在 Promise 之上。
边界：框架不对 Handler 做隐式重试，也不假设 Handler 是纯函数。Handler 内部的无界等待由业务负责，见 3.4。

**P3 存储可插拔，但不假装等价。**
后端可替换，框架按后端的能力声明降级，不假装不同后端语义等价。
边界：声明之外的行为属于未定义。框架只承诺不会声称自己提供了后端没有的能力，不为后端的能力差异背书。见 4.1。

**P4 进程生命周期管理。**
连接与业务进程解耦，业务进程可替换、可重启，连接不因业务进程重启而断开。
边界：框架负责发现进程不在、重启并重放未确认输入；不负责业务进程内部状态在重启后的正确性，那属于业务的幂等设计。

**P5 可观测。**
框架产出结构化日志与稳定错误码。
边界：稳定的是错误码与日志字段名，不是日志文案；框架不承诺指标与追踪的具体后端。

## 2. 核心术语

代码标识符、日志字段与文档 MUST 使用本节的定义，不引入同义词。类型名、类名、进程名与导出函数名是例外：它们标识实现构件，允许与术语不同，但每个不同的名字 MUST 在本节内显式声明它对应哪个术语（形如 Connection Process 下的「实现命名」段），且同一概念 MUST NOT 有两个实现名。文档用本节术语指代概念；使用实现名时 MUST 以本节声明的映射为依据，不得另起名字或随手互换。

**Request（请求）** — 客户端输入的最小处理单元，也是顺序保证与事务边界的基本单位。一条输入对应一个 Request；框架不把多条输入合并成一个 Request。

**Connection（连接）** — 一条具体传输链路上的连接实例。Connection 断开不等于 Session 结束。

**Session（会话）** — 玩家的一次逻辑在线期，由 Session ID 标识，可跨越多个 Connection（断线重连）。Session 是路由与状态归属的单位。

连接层保存 `sessionId → connectionId` 活跃路由，路由表是 Connection 当前绑定身份的唯一来源。绑定与解绑由业务通过 `RequestContext.bindSession` / `RequestContext.unbindSession` 声明，只在请求成功时生效；框架要求非空 id，但不判断其业务合法性。一个 Connection 同时只持有一个 Session；新 Connection 接管同一 Session 后，旧 Connection 保持连接但不再持有该 Session，只有再次显式绑定才会重新获得身份。连接断开时清除其路由，但不得因此丢弃该 Session 尚未结束的请求屏障（见 3.2）。边界：框架不持有 Session 的业务状态，其保留窗口与结束规则归业务进程。

**Transport（传输）** — 传输实现的接口：接受连接、交付入站数据、写出发站数据、关闭。它只负责字节流与传输帧的编解码（交付入站数据、写出发站数据的一部分），不含 Envelope 编解码、Session 路由与输入缓冲；Envelope 与客户端线格式见 §5.4，其余职责属于连接层进程。框架核心不依赖任何具体 Transport。

**Handler（处理器）** — 处理单个 Request 的业务函数，可以是 async。输入是已解码的请求与请求上下文；正常返回即表示该请求成功。注册时声明 `access`（默认 `'session'`，即要求请求携带 Session；`'public'` 不要求）与 `mayChangeSession`（默认 false，只有声明 true 的 Handler 可以调用 `bindSession` / `unbindSession`）。

**Extension（扩展）** — 业务能力的打包单元，向框架注册 Handler 并声明自己依赖的存储能力。框架核心不含任何游戏规则。

当前形状：`name`（日志中的唯一标识）、`requires`（存储能力最低要求，省略即在任何后端上运行）、`required`（是否为本进程启动所必需）与 `register(registrar)`。registrar 由框架提供，自动标注 handler 所属的 Extension，使请求日志能带上它（见 §7.1）。

**Storage Backend（存储后端）** — 实现存储契约并给出能力声明的持久化实现，以独立包发布。见 4.1。

**Capability Declaration（能力声明）** — 后端自述支持哪些存储能力的数据结构，框架据此选择降级路径。见第 4 节。

**Business Process（业务进程）** — 承载 Extension、Handler 执行与存储访问的进程，与连接层进程分离，是存储的唯一写者。

**Connection Process（连接层进程）** — 只负责传输接入、Session 路由与输入缓冲的进程。它不执行 Handler，不访问存储。

实现命名：Connection Process 在框架实现里叫 **Portal**（`ConnectionLayer` 与 `IpcExecutor` 两个类的组合），Business Process 叫 **Server**（`runServerProcess` 启动的进程）。后文使用这两个名字时代指本节的两个术语。

**Input Queue（输入队列）** — 每个 Connection 上的有界请求队列，保证同连接内请求有序。见 3.5。

**Request Context（请求上下文）** — 随 Request 传入 Handler 的框架侧上下文，携带 Session ID、Connection ID、请求标识、事务句柄、中止信号、日志器与 Extension 名。Handler 的存储调用与日志调用 SHOULD 经由它，框架据此统一归属日志、统一判定事务是否仍有效。

`bindSession` 与 `unbindSession` 只在上下文中记录变更，一个请求至多一次；变更仅在请求成功且未超时时交给调用方，由连接层在结果返回后应用。失败、超时或取消的请求不返回变更，因此失败的登录不会留下半建成的路由，失败的登出也不会移除已有路由。

中止信号是标准 `AbortSignal`，超时、取消或断线时 abort，`reason` 说明触发原因；正常成功不会触发异常中止。它是通知而非抢占：handler 可把它传给接受信号的操作以提前停下。事务句柄在请求的任一终态后均失效，不依赖 handler 是否响应信号；这不撤销已经开始的 COMMIT 或框架外副作用（见 3.3、3.4）。

**Terminal State（终态）** — Request 的结局，只有三类：成功、业务失败、框架失败（超时、取消、存储不可用、传输在完成前断开）。终态表示处理结束、结果已确定；它不等于已确认。

**Replay（重放）** — 业务进程崩溃后，把已接收但未确认的 Request 重新投递给重启后的业务进程。见 6.4。

**Acknowledged（已确认）** — Request 的终态结果已交回连接层的状态。终态与确认之间有一个窗口：结果已确定但尚未交回连接层时进程崩溃，该 Request 仍是终态却未确认。恢复语义基于确认而非终态——只有已确认的才能从重放缓冲中移除。见 6.4。

## 3. 执行模型

### 3.1 顺序

同一 Connection 上的 Request MUST 按接收顺序推进：Request N 到达框架终态后，N+1 才能开始处理。成功终态必须发生在该请求的事务提交之后；超时、取消与已开始 COMMIT 的竞态边界见 3.4。

理由：玩家的连续指令依赖前一条的结果（先"拿起剑"再"装备它"）。乱序会让业务状态不可推理，而且这类错误在业务层极难复现。框架不规定顺序的实现方式（串行 await、单消费者队列或其它），只规定可观测的结果。

### 3.2 并发

不同 Connection 之间 MUST 并发处理。

同一已绑定 Session 的框架非终态请求 MUST 串行处理。请求按开始处理时的入口 Session 归属；旧 holder 被接管后，其新请求不再携带原 Session，已开始请求的入口身份不变。新 holder 上属于该 Session 的请求必须等已有请求的 executor Promise 到达终态，不得以等待超时为由并发放行。

Session 的终态屏障独立于活跃路由：连续接管、旧连接断开或路由清除均 MUST NOT 丢弃尚未满足的先决屏障。绑定在成功结果返回后应用，不追溯改变本次请求的入口归属；首次从无 Session 发起的绑定请求不在目标 Session 的串行保证内。业务必须自行保护这种登录或绑定逻辑的共享状态。

边界：终态不等于 JS handler 已停止。超时或取消后，非合作 handler 可能继续计算，其框架外副作用与已开始 COMMIT 的竞态不在串行保证内（见 3.4）；框架不提供跨 Connection 的事务一致性或 Session 业务状态。

理由：跨连接串行会抹掉异步模型的全部收益；同 Session 并行则让"谁的状态更新生效"变得不可判定。

框架不提供跨 Connection 的事务。需要跨连接一致性的场景，由 Extension 使用存储层的并发原语处理，并自行承担冲突。并发承诺止于存储边界，见 4.4。

### 3.3 事务边界

每个 Request 隐式包在一个事务里，Request 与事务一一对应：

- Handler 正常返回 → 提交。
- Handler 抛错、超时或被取消 → 回滚。
- 同一 Request 内的多次存储调用 MUST 共享同一事务上下文，不得各自独立提交。

上述提交与回滚以存储后端声明的事务能力为限，降级规则见 4.3。取消或超时遇到已经开始的 COMMIT 时不能保证回滚，见 3.4。

`RequestContext.storage` 只在本 Request 未到终态时有效。成功、业务失败或框架失败后，保留的事务句柄 MUST 拒绝后续读写，MUST NOT 借后续 Request 的事务继续操作；事务句柄失效不意味着正常成功要触发 `AbortSignal`。

理由：Request 是业务能理解的原子单位。让业务手写事务边界会把事务管理成本扩散到每个 Handler。

Handler 内 MUST NOT 开启新事务。需要多段原子性的业务应拆成多个 Request。

### 3.4 超时与无界等待

每个 Request MUST 有超时上限，由框架配置，不由 Handler 自行决定。实际生效的超时取 `min(框架配置, 后端声明的 maxTransactionDurationMs)`：后端声明的是它自身能承受的上限，框架不得超过它，因此更短的一方生效。

超时后的语义：

- 超时到达即把该 Request 置为终态（框架失败，超时错误码），并使其事务上下文失效。
- 此后通过该上下文发起的读写 MUST 被拒绝，不得进入存储层。
- 框架 MUST NOT 继续等待该 Handler。Connection 可以立即处理下一个 Request。
- 未进入提交阶段的事务在 handler 返回后尝试回滚，强度以 4.3 的后端声明为限；超时不保证事务已回滚或 handler 已停止。

JS 无法抢占执行中的 Handler。超时保证结果确定与后续事务调用失效，不撤销已经发生的写入，也不保证框架外副作用停止。`RequestContext.signal` 同时 abort（`reason` 为 `'timeout'`），只是提前停止通知。

已经开始的 COMMIT 是明确边界：`StorageBackend.transaction(fn)` 在 `fn` resolve 后提交，取消或超时可能发生在提交完成之前，框架没有中止该 COMMIT 的句柄。此时事务仍可能提交，并记录 `CANCEL_LOST_TO_COMMIT` 告警（ADR-0004）；MUST NOT 把超时或取消当作"未生效"的证明。

事务内 MUST NOT 出现无界等待，包括：等待另一个 Request 的结果、等待用户输入、等待无超时的外部网络调用、等待一个无上界的锁。理由：事务持有后端资源或锁，一次无界等待会把单请求超时放大成整条连接、乃至整个后端的停顿。

### 3.5 有界队列

每个 Connection 的 Input Queue MUST 有明确上限。队列满时的行为 MUST 是显式策略之一：拒绝新输入并返回稳定错误码，或对传输层施加背压。MUST NOT 静默丢弃——静默丢弃会让玩家指令凭空消失且不可诊断，这是最难排查的一类线上问题。

队列满时采用拒绝策略：拒绝新输入并返回 `FRAMEWORK_QUEUE_FULL`，不做背压。理由是拒绝是队列自己能兑现的唯一策略：它让请求立刻以稳定错误码失败，而不是把整条连接堵在一个满队列后面。

### 3.6 取消与断线

Connection 在 Request 处理中关闭时，该 Request MUST 进入终态，不得无限期挂起。

策略是取消，没有例外：中止信号触发，事务上下文失效，Request 以框架错误结束。未进入提交阶段的事务在 handler 返回后尝试回滚；已经开始的 COMMIT 仍受 3.4 的边界约束。选择取消的理由是玩家已无法接收结果，不能让后续事务调用继续生效。

框架不提供不可取消声明。Portal 在断线时结算请求，并清除连接持有的路由；若 Server 可拒绝取消，Portal 就需要额外的取消确认协议才能区分"仍在执行"与"已到终态"，否则重连后的 Session 屏障无法据此决策。当前协议不提供这条分支。

需要不可回滚副作用的业务必须自行保证幂等与重试安全，不能把断线取消或超时当作副作用未发生的证明。

关闭流程等待框架在途请求的终态，不等待非合作 handler 的全部后台计算。正常关闭受 Portal 声明的关闭预算约束，链路中断后的 abort 路径受 `ABORT_TIMEOUT_MS` 约束，超时只放弃等待（见 6.3）。取消使后续事务调用失效、让结果不再送达，不保证 handler 已停止、事务已回滚或已开始的 COMMIT 被撤销。

断线不等于 Session 结束。Session 状态在业务进程侧保留一段时间（由配置决定），期间重连可恢复；超时未重连则按业务规则结束。

### 3.7 访问检查

Handler 的访问检查在业务进程内、Dispatcher 上执行，顺序是契约：

1. 未注册的 method 以 `UNKNOWN_METHOD` 结束，不因请求缺少 Session 改成 `AUTH_REQUIRED`。
2. Handler 默认 `access: 'session'`；请求的入口 Session 缺失或为空时，以 `AUTH_REQUIRED` 拒绝并记录 warn（`code`、`method`）。`'public'` 跳过这一步。
3. 若消费方注入了 `authorize(ctx, method, payload)`，对所有已注册 Handler（包括 public）调用；返回 false 以 `FORBIDDEN` 拒绝并记录 warn，抛出的异常按框架内部失败处理（`INTERNAL_ERROR`）。策略只读取请求身份、`AbortSignal` 与日志器，不获得存储或 Session 变更能力，且在请求 deadline 内完成。
4. 打开存储事务并执行 Handler。

拒绝 MUST NOT 打开存储事务、执行 Handler 或改变 Session 路由。Handler 未声明 `mayChangeSession: true` 却调用 `bindSession`/`unbindSession` 时，校验抛出普通错误；handler 未捕获时请求以 `INTERNAL_ERROR` 失败；handler 捕获后请求可以成功，但不会记录任何 Session 变更——吞掉该错误不构成合法用法。检查点与拒绝语义属于框架；认证与授权策略、登录流程属于消费方，由消费方注入或实现为 public Handler。

## 4. 存储契约

### 4.1 接口边界

框架包 MUST NOT 包含任何存储后端实现，MUST NOT import 具体后端包。存储接口（读、写、事务开启/提交/回滚）与能力声明类型的签名稳定；能力差异通过能力声明表达，而不是通过接口分叉或运行时嗅探。新增后端不得要求改动框架核心。

理由：后端与具体驱动、方言、连接模型强绑定，发版节奏与框架不同；放在同一个包里，任一方升级都会拖累另一方。

### 4.2 能力声明

后端 MUST 导出能力声明。当前约定的字段：

```ts
interface StorageCapabilities {
  /** full：多实体原子；limited：仅单实体/单键原子；none：无回滚 */
  transactions: 'full' | 'limited' | 'none';
  /** single：单连接串行；pooled：连接池并行 */
  concurrency: 'single' | 'pooled';
  /** 事务内读是否为一致快照 */
  snapshotRead: boolean;
  /** 是否提供原子唯一约束（检查与写入不可分割） */
  uniqueConstraint: boolean;
  /** 后端自身的事务时长上限；框架不得超过它。必填，无上限时显式声明为 Infinity */
  maxTransactionDurationMs: number;
}
```

所有字段都是必填。能力声明是后端与框架之间的契约，缺任何一个字段都会让框架的降级判断读到 undefined；无法提供的值必须显式声明为最弱的一档（如 `transactions: 'none'`、`maxTransactionDurationMs: Infinity`），而不是省略。能力名只增不删；新增能力字段与删除、改名能力名都属于破坏性变更，MUST 同步升级所有后端，见第 8 节。

框架 MUST NOT 假设后端具备未声明的能力。

### 4.3 降级规则

框架按声明选择路径，并把降级事实记录为告警级日志。对每个降级维度 MUST 记录一条 `warn`，在后端进程启动时各记录一次，不按请求刷屏；该日志行 MUST 包含 `capability`（能力名）、`declared`（后端声明值）、`consequence`（说明该维度要求业务承担的后果）三个字段。全强度声明 MUST NOT 产生告警。

实现：Server 进程启动时在能力自检通过后产出上述告警，同一次启动另有一条 info 记录完整能力声明，给出全貌；逐条 warn 给出后果，两者互补。告警文案与日志行的产出顺序不是契约（见 §7.1）。

| 声明                    | 框架行为                                                                         | 业务需要承担的                                      |
| ----------------------- | -------------------------------------------------------------------------------- | --------------------------------------------------- |
| transactions: full      | 提供 3.3 的全部语义                                                              | 无需额外处理                                        |
| transactions: limited   | 事务只覆盖单实体，跨实体写入不保证原子                                           | 需要多实体原子性的 Extension 必须声明需求并拒绝降级 |
| transactions: none      | 无回滚；失败时中止后续写入，已写入的部分保留                                     | Extension 必须自行实现补偿或幂等                    |
| concurrency: pooled     | 框架层并发调度可落到存储层并行执行                                               | 无需额外处理                                        |
| concurrency: single     | 框架层并发调度不转化为存储层并发，多个事务在后端内部串行执行，吞吐受后端串行限制 | 不得依赖并发写入提升吞吐；长事务会直接阻塞其他请求  |
| snapshotRead: false     | 框架不承诺事务内重复读一致                                                       | Extension 自行处理读到变化数据的情况                |
| uniqueConstraint: false | 框架不提供"检查再写入"的原子性                                                   | 唯一性由单写者或业务侧幂等键保证                    |

Extension 对存储能力的依赖 MUST 显式声明。启动时若后端能力不满足某个 Extension 声明的最低要求，框架 MUST 拒绝注册该 Extension（fail fast），不影响其他 Extension。被拒绝的 Extension 不执行注册，其 Handler 全部不存在；声明 `required` 的 Extension 被拒绝时整个业务进程拒绝启动，因为缺少它的进程不是可用的进程。

依赖声明的粒度是"最低要求"，不是"完整能力清单"。例如银行类 Extension 要求 `transactions: full` 与 `uniqueConstraint: true`；只读查询类 Extension 可以不声明任何存储能力要求，从而在任何后端上运行。

理由：静默降级会在最需要原子性的时刻（崩溃、并发）暴露问题，而那时已经无法回头修改设计。

### 4.4 单连接后端

`concurrency: single` 的后端 MUST 在内部串行化事务，否则一方回滚会吞掉另一方已写入的数据。这是正确性要求而非性能优化；框架 MUST NOT 感知该排队。

### 4.5 后端职责

一个后端至少实现基础读写与事务开启/提交/回滚。能力声明决定这些操作的实际保证强度，不决定它们是否存在；后端 MUST 如实声明，不得为了通过启动校验而虚报。框架测试 MUST 使用内存假后端，MUST NOT 依赖真实数据库。

### 4.6 写者

业务进程是存储的唯一写者，连接层进程 MUST NOT 直接写存储。若将来引入多写者，MUST 显式声明并给出冲突解决策略，不得以"后端能处理"为由默认放开。

### 4.7 不做 ORM

存储契约是数据访问的最小面，不是对象映射层。框架不提供实体关系图、级联、延迟加载或自动迁移。

理由：MUD 的状态结构由业务演化，任何框架级映射层都会在某个版本后变成阻碍。后端只需能存取值、能开事务，剩下交给 Extension。

## 5. 传输契约

### 5.1 接口边界

框架核心 MUST NOT 包含任何具体传输实现，MUST NOT import 具体传输包。`Transport` 接口的签名稳定。`connector-ws` 等实现是独立包，独立发版、独立测试。新增传输只需实现接口，不得要求改动框架核心。

理由：传输与具体协议、帧格式、握手强绑定，发版节奏与框架不同；放在同一个包里，任一方升级都会拖累另一方。

### 5.2 传输职责边界

Transport 只负责四件事：接受连接、交付入站数据、写出发站数据、关闭。MUST NOT 承担：Session 路由、输入缓冲、重放、访问检查、TLS 终止。理由：这些职责依赖 Session 与 Request 语义，属于框架契约；下沉到传输会让每个传输实现各自重写一遍，且无法保证跨传输一致。

职责拆分：Session 路由、输入缓冲与重放在连接层进程（见 6.1）；访问检查点与拒绝语义（`AUTH_REQUIRED` / `FORBIDDEN`、检查顺序）在 core 的 Dispatcher 内，随 Handler 在业务进程执行（见 3.7）；认证与授权策略、登录流程属于消费方，由消费方注入 `authorize` 或在 Extension 中实现；TLS 终止在部署层或具体传输实现，core 不承担。

### 5.3 独立维护边界

- 传输实现升级不触发框架核心发版；框架只保证 `Transport` 接口稳定，接口变更属破坏性变更（见 8.2）。
- 框架测试 MUST 使用假传输，MUST NOT 依赖真实网络。

### 5.4 客户端线格式

客户端与连接层之间的线格式属于框架契约，以 `Codec` 接口与 `Envelope`、`ResponseEnvelope` 类型为准。字段名是契约；默认的 `JsonCodec` 以 JSON 编码，替换 `Codec` 即替换编码方式，框架的其余契约不随之改变。

请求是 `Envelope`：`{id, method, payload}`。`id` 是客户端提供的关联标识，响应原样回填；`method` 命名一个 Handler；`payload` 交给 Handler 解释。响应是 `ResponseEnvelope`：`{id} & ({ok: true, result} | {ok: false, error: {code, message}})`。`payload` 与 `result` 对框架不透明——框架不解析、不改写。客户端的 `id` 不是 §7.3 的请求标识：后者由框架在入口分配，不出现在客户端线格式中。

`ok` 是必需的判别字段，不能由可选字段的组合代替：`{"id":"x","ok":true}` 让"成功且无返回值"成为显式判别分支；JSON 编码会丢弃值为 `undefined` 的字段，省略 `ok` 后该响应与缺少 `ok` 的非法帧无法区分。失败分支的 `error` 只有 `code` 与 `message`，`code` 的取值见 §7.2，`message` 是供人阅读的说明而非稳定契约。

默认 `JsonCodec` 的入站校验：解码结果必须是对象，`id` 与 `method` 必须是非空字符串；其余校验细节由 `Codec` 实现决定。

连接层正常接受输入时，入站帧解码失败不构成 Request，但 MUST 占用有界输入队列的席位。取得席位的坏帧按连接顺序处理：记录一条 warn（`code` 为 `INVALID_ENVELOPE`，字段含 `connectionId`、`rawLength`、`reason`），并以 `id` 为空字符串的失败响应回 `INVALID_ENVELOPE`，不从坏帧中提取关联标识，也不因一帧坏数据终止连接。队列已满时，坏帧立即以 `id` 为空字符串的 `FRAMEWORK_QUEUE_FULL` 拒绝，不追加排队任务；这类拒绝响应可以越过已接受输入，只有已取得席位的输入遵守排队顺序。

关闭流程中（连接层状态为 stopping 或 stopped）新到达、尚未进入 Input Queue 的输入一律以 `FRAMEWORK_SHUTDOWN` 拒绝，包括无法解码的帧，不进入请求处理；已接受的输入按 6.3 排空或显式结算，不保证全部执行。能解码的帧回填其 `id`，无法解码的回 `id` 为空字符串。关闭期间未跟踪连接上的输入同样收到 `FRAMEWORK_SHUTDOWN`；未跟踪连接上、且不在关闭流程中的输入没有响应：连接层没有该连接的活跃记录，不为它解码或回写。

`result` 无法被当前 `Codec` 编码时，框架用同一 `Codec` 编码一条 `INTERNAL_ERROR` 失败响应作为兜底；若该兜底仍无法编码，框架只能记录 `TRANSPORT_SEND_FAILED`，不承诺发出响应。

## 6. 生命周期

### 6.1 进程关系

连接层进程与业务进程分离。连接层持有传输连接、Session 路由、Input Queue 与未确认输入缓冲；业务进程持有 Extension、Handler 执行与存储访问。

分离的理由不是架构美观，而是崩溃恢复：业务进程崩溃后可以重启并重放，连接层保住玩家不断线。

状态归属随之确定：Session 的业务状态归业务进程，连接层只保存路由信息与未确认输入。连接层重启会断开连接，但不会丢失已提交的业务状态。

连接层对同时活跃的连接数设准入上限（默认 10000）。达到上限时，新连接 MUST 被关闭且不进入请求处理；连接断开后空出的名额 MUST 可供后续连接使用。该上限只约束准入，不是业务 Session 的生命周期限制。理由：绑定在 Handler 成功后才声明，连接层无法在执行前预检 Session 容量；活跃连接数是唯一能在副作用前拒绝的位置，而路由表大小不会超过连接数。

### 6.2 启动顺序

1. 存储后端初始化并自检能力声明。
2. 业务进程加载 Extension，按 4.3 校验其声明的存储能力；业务进程启动所必需的能力不满足时拒绝启动业务进程。
3. 连接层开始接受连接。

连接层 MUST NOT 在业务进程就绪之前接受输入。理由：否则第一批玩家会收到"框架正在启动"这类本不该存在的错误。

就绪由两阶段握手表达（ADR-0007）：业务进程收到 `hello` 后先回 `prepared`，此时 MUST NOT 打开存储、注册 Extension 或执行 Handler；只有收到 `activate` 才执行第 1、2 步并安装请求处理器，然后发 `ready`。连接层在收到 `ready` 之后才开始接受连接。边界：`prepared` 只表示进程活着且等待激活，不表示可用；连接层不得向 prepared 的业务进程投递 Request。

初次 `start()` 的整体等待受 `startTimeoutMs` 约束，期间的失败重试不延长总时限；自动恢复启动的每一代另有同值的独立握手时限，从该代启动到 `ready`。计时器 MUST 绑定 generation，在该代 ready、丢失或关闭后失效，不得影响其他代或使终态执行器复活。切换的 prepared 与 ready 两个等待分别有界（ADR-0007）。

版本切换复用同一协议，顺序是契约：连接层先起新进程并等到 `prepared`，再停旧进程、等它 `closed` 且退出被观测到，然后才向新进程发 `activate` 并等 `ready`，最后把缓冲请求投给新进程。`activate` MUST NOT 在旧进程仍可能持有存储时发出（INV-002）；`closed`、kill 返回与退出等待超时都不能代替退出确认。切换期间连接层 MUST 继续按容量与 deadline 接受、缓冲请求，不得向即将退役的旧进程或尚未 `ready` 的新进程投递；新进程 `ready` 后按 6.5 的顺序投递。

切换失败时，旧进程未被 drain 则 MUST 继续服务；已经退役则使用切换前的 spawn 与 `codeVersion` 恢复。回滚重启 MUST 等到旧进程与已 `activate` 的新进程退出均被观测到；无法确认时保持写者屏障并显式失败，不得以恢复可用性为由启动另一写者。切换成功提升后，连接层 MUST 采纳目标 spawn 与 `codeVersion`，供后续崩溃重启与新请求使用；失败回滚不改变当前版本。`close()` 在 `activate` 之后中止切换时，MUST 对放弃的新进程执行有界的退出确认（等退出 → kill → 再等）；确认失败时关闭必须 reject，而不是只记日志后成功返回。

### 6.3 关闭顺序

1. 连接层停止接受新输入。
2. 排空已接受输入，受连接层关闭预算约束；仍在 Session 屏障前、尚未调用 executor 的请求以 `FRAMEWORK_SHUTDOWN` 结束，不绕过屏障继续执行。
3. 执行器等待已接受工作，并关闭 Server 与存储，所有等待有界；超时或失败必须可观测，不得当作已确认资源释放。
4. 尽力关闭传输，即使执行器关闭已经失败也不得跳过。

`ConnectionLayer.stop()` MUST 在传输收尾后传播执行器关闭失败，不得以成功返回掩盖退出未确认。执行器有界返回不等于进程已经退出；旧写者的退出未被观测时 MUST NOT 启动新的写者。完整外部停止预算必须覆盖连接层排空、执行器关闭与传输关闭三段，不能只使用 `IpcExecutor.closeBudgetMs`（公式与常量来源见 ADR-0004）。

顺序的理由是先结束业务调度，再释放存储与传输；但关闭错误码不证明请求未执行或未提交，阶段边界见 7.2。

### 6.4 崩溃恢复

承诺：连接保活 + 未确认输入重放。终态与确认的区别见 §2 术语；重放缓冲的移除条件是确认，不是终态。

恢复受唯一写者屏障约束：只有 `SpawnedServer` 报告退出 `observed: true` 才能解除旧写者屏障。退出 Promise 仍 pending 而等待超时时，框架必须保留引用，迟到的确认可在执行器仍可恢复时继续一次恢复；`close()` 或其它终态之后不得被迟到事件重新启动。若一次性退出结果是 `observed: false`，框架无法据此确认进程释放了存储，必须保持屏障，不承诺自动恢复；有界关闭仍无法确认退出时必须失败。恢复可用性不能优先于 INV-002，请求仍受各自 deadline 约束。

语义是 at-least-once：同一个 Request 可能被执行多次。框架 MUST NOT 声称 exactly-once。

幂等由业务负责。需要"只生效一次"的效果（扣钱、发物品、唯一命名），必须由 Extension 用业务幂等键实现，不能依赖框架。

理由：at-least-once 是"连接保活"这一承诺的直接代价——只要允许重放，重复执行就无法在框架层排除。这也是双进程架构存在的理由：单进程崩溃会同时带走连接与状态，无从重放。

代码版本只回显，不判兼容：`prepared`、`ready` 与 `request` 上的 `codeVersion` 是应用提供的 opaque 非空字符串。`prepared` 与 `ready` 报告同一个值——该 Server 正在运行的代码版本；`request` 记录 Portal 接受该请求时的版本，重放沿用该值而不是替换 Server 的版本。Portal 在自身配置了版本、且 `ready` 带了一个不同版本时记 warn（`expectedCodeVersion`、`actualCodeVersion`、`pid`）；Server 在处理 `replayCount > 0` 的请求、且请求版本与自身版本都存在且不同时记 warn（请求标识字段、`requestCodeVersion`、`serverCodeVersion`、`replayCount`）。两侧都不因不匹配失败、拒绝、取消请求或改变重放策略。跨版本重放只保证 at-least-once 与可观测，不保证行为兼容；是否允许带版本热修、如何发布，由部署方决定。切换成功后 Portal 的"自身版本"变为切换目标版本（`switchTo` 的 `codeVersion`），崩溃重启也使用目标 spawn；切换失败回滚则不变。

### 6.5 重放的顺序约束

重放 MUST 保持原顺序：同一 Connection 上未确认的 Request 按接收顺序重放。理由：重放若乱序，等于在恢复路径上重新引入 3.1 已经排除的问题，而且发生在最不稳定的时刻。

重放不是无限重试。同一 Request 连续失败到上限后 MUST 进入终态并记录错误，不得无限重放——那会把一个坏输入变成对业务进程的持续攻击。

## 7. 可观测性

### 7.1 结构化日志

框架日志 MUST 是结构化的（字段而非拼接字符串）。字段名是契约，文案与日志行的产出顺序不是契约。每个 Request 的日志 SHOULD 至少携带：请求标识、Session ID、Connection ID、Extension 名、耗时、结果、错误码。日志级别按语义使用：告警级用于能力降级、重放、队列拒绝这类"能继续运行但需要人知道"的事件；错误级用于请求失败；不得用错误级记录预期内的业务拒绝，包括 `AUTH_REQUIRED` 与 `FORBIDDEN`（见 3.7、7.2）。

注入的日志实现抛错 MUST NOT 改变框架控制流。日志实现失败时框架不保证 `child()` 的绑定字段（bindings）完整：`safeLogger` 的 `child` 抛错会回退到父包装。

已实现：`RequestContext.extension` 由 handler 的注册元数据填充。通过 `ExtensionRegistrar` 注册的 handler 自动带上所属 Extension 名，请求日志的 bindings 仅在存在该名时写入 `extension` 字段；不属于任何 Extension 的 handler 不带该字段。

边界：`extension` 字段只有 Server 侧在 handler 注册元数据可知时填充。Portal 不持有 handler 注册信息，其请求级日志（例如拒绝、队列满、超时、取消、重放）无法获知 Extension，不能按该字段过滤 Portal 日志；这些日志与 Server 侧日志依靠请求标识与 Connection ID 关联。

边界：请求结算之后产生的日志（如 `late result discarded`）依靠结果消息恢复 Session ID。结果消息回显请求进入框架时的入口 Session ID（`requestSessionId`，成功与失败结果都携带；入口无 Session 时不带），迟到结果用它填 `sessionId`；结果消息的 `sessionChange` 仍只表示 handler 在本次请求内声明的 Session 变更（bind 携带新 Session，unbind 表示解绑）。结果未带 `requestSessionId` 时，已结算的日志退回用稳定的请求标识（§7.3）与同一请求的其它日志行关联。

结果/完成类日志（`request completed`、`result received`、失败日志）的 `sessionId` 表示请求入口 Session（与 `RequestIdentity` 同义），不会因 Handler 绑定而被改写；入口无 Session 时不写。成功请求声明了 Session 变更时写 `sessionChange`，值为 `SessionChange` 对象（`{type:'bind',sessionId}` 或 `{type:'unbind'}`）；没有变更不写。字段名与语义都是契约。路由事件行（`session bound`、`session rebound to a new connection`）的 `sessionId` 表示本次被建立或接管的路由 id，与请求入口 Session 无关。

### 7.2 错误码

错误码 MUST 稳定、可枚举、只增不删。语义变更 MUST 分配新码，不得复用旧码表示新含义。

错误码分两类，约束不同：

**wire 码** 随 `error.code` 发给客户端（`ok: false` 时），属于对外契约，MUST 稳定可枚举：

| 码                                 | 含义                                         |
| ---------------------------------- | -------------------------------------------- |
| `FRAMEWORK_TIMEOUT`                | 请求超时                                     |
| `FRAMEWORK_QUEUE_FULL`             | 输入队列拒绝（见 3.5）                       |
| `FRAMEWORK_SHUTDOWN`               | 因关闭未取得成功确认，不保证未执行或未提交   |
| `FRAMEWORK_REPLAY_EXHAUSTED`       | 重放次数用尽，请求进入终态（见 6.5）         |
| `FRAMEWORK_CAPABILITY_UNSUPPORTED` | 存储能力不满足 Extension 声明（见 4.3）      |
| `TRANSPORT_CLOSED`                 | 传输在请求完成前关闭                         |
| `TRANSPORT_SEND_FAILED`            | 结果无法写回传输                             |
| `STORAGE_UNAVAILABLE`              | 存储后端不可达                               |
| `INVALID_ENVELOPE`                 | 入站帧无法解码                               |
| `UNKNOWN_METHOD`                   | 没有该方法的 Handler                         |
| `INTERNAL_ERROR`                   | 框架内部失败，含未声明的业务错误             |
| `AUTH_REQUIRED`                    | Handler 要求 Session，但请求未携带（见 3.7） |
| `FORBIDDEN`                        | 注入的访问策略拒绝了请求（见 3.7）           |

`AUTH_REQUIRED` 与 `FORBIDDEN` 是预期内的拒绝，不是框架失败：它们发生在存储事务与 Handler 之前，日志级别为 warn（见 7.1），MUST NOT 按错误级记录。它们在业务进程的访问检查点产生，证明本次请求没有打开事务、执行 Handler 或改变 Session 路由。

`FRAMEWORK_SHUTDOWN` 必须区分发生阶段：关闭时拒绝的新输入，以及仍在 Session 屏障前、尚未调用 executor 就被关闭结算的请求，确定没有被调度执行；可能已投递给 Server 却未取得成功确认的请求，则可能已经执行或提交。此码本身不携带阶段信息，MUST NOT 仅凭它判断可无幂等保护地重试；恢复与重试仍须遵守 6.4 的业务幂等边界。

`FRAMEWORK_CAPABILITY_UNSUPPORTED` 与 `STORAGE_UNAVAILABLE` 是预留码：当前没有任何路径产出它们，先占住名字，免得实现时另发明一个码让下游同时面对两套语义。

`TRANSPORT_SEND_FAILED` 的触发条件是"写不出去"，因此它结构上不可能送达客户端，只能作为日志字段被观察到。`TRANSPORT_CLOSED` 在当前实现里同样只出现在日志中：请求因断线结束时连接已不在，没有可回写的通道。两码仍是契约——它们回答"请求为什么结束"，删掉会让终态失去可枚举的说法。

**仅日志诊断字段** 出现在日志的 `code` 字段里，用于定位，不发给客户端，也不构成 wire 契约：`CANCEL_LOST_TO_COMMIT`、`IPC_CHANNEL_CLOSED`、`SERVER_STARTUP_FAILED`、`DUPLICATE_REQUEST`、`INVALID_IPC_MESSAGE` 等。日志里的 `code` 只是一个字段名，取值不受本节约束——把它当成错误码清单会误判哪些码是契约。

`FRAMEWORK_` 前缀是示例惯例，不是强制格式。`INVALID_ENVELOPE`、`UNKNOWN_METHOD`、`INTERNAL_ERROR`、`TRANSPORT_SEND_FAILED` 没有该前缀；错误码只增不删（INV-014），改名等于换码，所以这些名字保持不变。

业务错误码由 Extension 用 `BusinessError` 显式定义，框架原样透传，不改写、不吞掉。只有 `BusinessError` 的 `code` 与框架自身错误码进入 wire：驱动或运行时的偶然 `code`（如 `ENOENT`、`SQLITE_CONSTRAINT_UNIQUE`）不透传，统一降级为 `INTERNAL_ERROR`。否则第三方库升级会静默改变对外契约（INV-014）。

### 7.3 请求标识与关联

每个 Request 在进入框架时分配请求标识，并在整个生命周期内保持不变——包括跨进程投递与重放。理由：一次重放会产生多条日志，分布在两个进程；若重放时换新标识，就无法回答"这条日志对应玩家哪一次输入"，而这正是排障时最先要回答的问题。重放时 SHOULD 额外记录重放次数与原始接收时间，用于识别"同一个请求反复失败"的情况。

## 8. 变更规则

### 8.1 代码与文档冲突

代码是唯一真相。代码与本文冲突时以代码为准，本文 MUST 修正。

唯一例外是 INVARIANTS：那里写的是产品承诺，违反它是 bug，不是文档过期。判定属于"代码错了"还是"承诺变了"，由修改者显式给出结论，不允许默认按代码改文档。SPEC 与 INVARIANTS 冲突时以 INVARIANTS 为准。

### 8.2 改 SPEC 需要什么

- 改动承诺（P1–P5）或 INVARIANTS：属于重大变更，MUST 说明改了什么、为什么原承诺不再成立、受影响的代码与迁移方式。
- 改动边界或术语：MUST 同步修改代码标识符、类型定义与两份文档，不留同义词。
- 改动存储能力：只增不删；删除或改名能力名按重大变更处理。
- 改动 `Transport` 接口签名按重大变更处理，MUST 同步升级所有传输实现。
- 纯补充说明：可直接改，不构成契约变更。

### 8.3 文档不写什么

本文不写模块职责表、目录结构、测试覆盖与实现步骤。这些随代码变化，写进本文只会过期。本文只写决策、边界与理由。

契约只存在于本文与 `docs/INVARIANTS.md`。仓库可以另有明确标注为非契约的操作文档（如 `docs/INTEGRATION.md`）：它们写可复制的代码、装配步骤与部署检查清单，随实现变化，不构成承诺；与本文冲突时以本文为准。
