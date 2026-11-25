# 双进程装配

框架分成两个进程（ADR-0002）：Portal 持有连接，Server 执行 Handler 并独占存储写入。两个进程必须由使用者自己装配——框架提供两端的函数，不提供进程管理器。本文只写装配决策与边界；协议与恢复语义见 ADR-0004。

本文是操作文档，不是契约：可复制的代码与部署检查清单属于这里，承诺与不变量属于 SPEC 与 INVARIANTS。与 SPEC 或 INVARIANTS 冲突时以它们为准；SPEC 与 INVARIANTS 的关系见 SPEC §8.1。

## 最小可运行装配

### Portal

```ts
import { WsTransport } from 'mud-framework-connector-ws';
import {
  ConnectionLayer,
  IpcExecutor,
  spawnBunServer,
} from 'mud-framework-core';

const executor = new IpcExecutor({
  spawn: () => spawnBunServer({ command: ['bun', 'run', './server/main.ts'] }),
  timeoutMs: 5000,
});

const transport = new WsTransport({ port: 8080 });
const layer = new ConnectionLayer({ transport, executor });

await layer.start();

process.once('SIGTERM', () => {
  void layer.stop().then(() => process.exit(0));
});
```

Portal 不碰存储：这里没有任何 backend。`layer.start()` 会先拉起 Server，等它回 `prepared` 后自动发 `activate`，再等到 `ready` 才开始接受连接（SPEC 6.2，两阶段见 ADR-0007）。

### Server

```ts
import { runServerProcess } from 'mud-framework-core';
import { SqliteBackend } from 'mud-framework-storage-sqlite';

try {
  await runServerProcess({
    createBackend: () =>
      SqliteBackend.open({ filename: './data/world.sqlite' }),
    extensions: [
      {
        name: 'players',
        requires: { transactions: 'full', uniqueConstraint: true },
        register: (registrar) => {
          registrar.register(
            'player.create',
            async (ctx, payload) => {
              const name = (payload as { name: string }).name;
              await ctx.storage.execute(
                'INSERT INTO players (name) VALUES (?)',
                [name],
              );
              return { created: name };
            },
            // 示例没有登录步骤，所以这个入口显式公开；真实业务通常只把
            // 登录/注册声明为 public，其余方法保持默认。
            { access: 'public' },
          );
        },
      },
    ],
    timeoutMs: 5000,
  });
} catch (error) {
  // 将启动或链路失败映射为非零退出，供进程管理与诊断识别。
  console.error(
    'server stopped abnormally:',
    error instanceof Error ? error.message : String(error),
  );
  process.exitCode = 1;
}
```

上面片段的前置条件是 `./data` 目录已存在，且数据库已有业务表 `players (name TEXT NOT NULL)`。`SqliteBackend.open` 既不创建父目录，也不创建业务 schema；目录缺失会导致打开失败，表缺失会让 `player.create` 返回 `INTERNAL_ERROR`。schema 初始化与迁移属于应用，必须在接受业务请求前完成；可放在 Server 的 `createBackend` 中，但不得提前到 Portal 或 prepared 阶段。`examples/minimal/` 是可直接运行的版本：它在 `createBackend` 中递归创建目录并用 `CREATE TABLE IF NOT EXISTS` 建表，首次运行无需手工准备。

### 访问声明与登录装配

Handler 默认 `access: 'session'`：请求没有 Session 时以 `AUTH_REQUIRED` 拒绝，且不会打开事务或执行 Handler。登录/注册这类入口声明 `access: 'public'` 并加 `mayChangeSession: true`，在 handler 内校验凭据后调用 `ctx.bindSession(id)`；该绑定只在请求成功后生效。登录之后的方法保持默认，连接层会把已绑定的 Session 带到后续请求。退出登录同理：public + `mayChangeSession: true` 的 handler 调 `ctx.unbindSession()`，成功后路由被移除，失败时保留。

凭据校验、用户数据、Session ID 的生成与保留策略都属于消费方，框架不提供用户库或登录协议。需要更细的授权判断时，单进程装配给 `Dispatcher` 注入可选的 `authorize(ctx, method, payload)`，双进程装配把它传给 `runServerProcess`（Server 创建 Dispatcher 时透传）：它在 Session 检查之后、事务之前执行，只看到请求身份、`AbortSignal` 与日志器；返回 false 得到 `FORBIDDEN`，抛错按 `INTERNAL_ERROR` 处理，且整个决策受请求 deadline 约束。

`runServerProcess` 返回的 Promise 就是退出信号：收到 `shutdown` 并正常收尾时 resolve，启动失败或链路断开时 reject。入口脚本应把失败映射为非零退出码，供进程管理与诊断识别；Portal 也通过 IPC 断开与退出事件发现 Server 丢失，不只依赖退出码。

## 健康探针（应用层）

core 只提供同步快照，不提供 HTTP 端点（SPEC 6.6）。应用在自己的 HTTP 服务里把 `layer.health()` 映射为探针：`ready === true` 返回 200，否则返回 503；需要区分原因时把 `state` 与 `executor.state` 写进响应。快照会立即过期，只表达“此刻可以接活”，不能替代请求准入或 deadline。端点属于应用，`examples/minimal/src/portal.ts` 给出了完整实现：独立 `Bun.serve` 监听 8081，响应体包含快照，Portal 停止时一并关闭。

## 版本化入口（应用层）

版本到入口的映射属于应用与部署配置，不进 core。应用自己维护 `codeVersion -> server entry`：Portal 的 `spawn` 闭包按当前版本选择入口，并把同一个版本传给 `IpcExecutor`；Server 入口把该版本原样交给 `runServerProcess`。框架不解析版本、不判断兼容性、不做版本到入口的映射。

```ts
import { IpcExecutor, spawnBunServer } from 'mud-framework-core';

const SERVER_ENTRY_BY_CODE_VERSION = new Map([
  ['2026-11-20', './server/main.ts'],
  ['2026-11-25', './server/main.next.ts'],
]);

const codeVersion = '2026-11-25';

function serverEntryFor(version: string): string {
  const entry = SERVER_ENTRY_BY_CODE_VERSION.get(version);
  if (entry === undefined) {
    throw new Error(`no server entry for code version ${version}`);
  }
  return entry;
}

const executor = new IpcExecutor({
  spawn: () =>
    spawnBunServer({
      command: ['bun', 'run', serverEntryFor(codeVersion)],
    }),
  codeVersion,
  timeoutMs: 5000,
});
```

```ts
// server/main.next.ts：入口随版本发布，声明值必须与映射中选它的键一致。
import { runServerProcess } from 'mud-framework-core';
import { SqliteBackend } from 'mud-framework-storage-sqlite';

await runServerProcess({
  createBackend: () => SqliteBackend.open({ filename: './data/world.sqlite' }),
  extensions: [/* 同最小装配 */],
  codeVersion: '2026-11-25',
  timeoutMs: 5000,
});
```

`codeVersion` 必须是应用提供的非空字符串：`IpcExecutor` 构造、`runServerProcess` 启动时都会校验，空串或非字符串直接失败。版本是否与实际构建产物一致、入口声明是否与映射键一致，由构建与部署保证，框架不校验。

回显与告警：

- Server 在 `ready` 中回显自己声明的版本，`prepared` 携带同一个值。Portal 配置了版本、且 `ready` 带了不同的版本时记 warn（`expectedCodeVersion`、`actualCodeVersion`、`pid`）；它仍然接受这个 Server，不据此拒绝启动或重启。
- Portal 在接受请求时把版本写入 `request.codeVersion`，重放沿用接受时的值，不会替换成当前 Server 的版本。Server 处理 `replayCount > 0` 的请求、且请求版本与自身版本都存在且不同时记 warn（请求标识字段、`requestCodeVersion`、`serverCodeVersion`、`replayCount`）。
- 任一侧未声明版本时不算"不同"，不记 warn；缺失字段表示该部署未声明版本。
- 版本不匹配只影响可观测性：框架不据此拒绝、取消、路由请求或改变重放策略。跨版本重放仍按 at-least-once 执行，行为兼容不在承诺内；是否允许带版本热修、如何发布，由部署方决定。

## 硬约束

### 子进程必须是 Bun 进程

`spawnBunServer` 使用 `Bun.spawn` 的 `ipc` 选项，其序列化格式只与其他 Bun 进程兼容。用 `node` 启动 Server 不会报错：`process.send` 存在，消息被写入，但 Portal 侧收不到任何 IPC 回调，表现为启动永久等待 `ready`，直到 `startTimeoutMs` 才失败。`command` 的第一个元素按 `PATH` 解析，因此部署环境里 `bun` 必须在 `PATH` 上。

### 子进程不得往 stdout 写

`spawnBunServer` 的 `stdio` 是 `['ignore', 'inherit', 'inherit']`：子进程的 stdout 直接继承 Portal 的 stdout，stderr 也是。IPC 走独立通道，往 stdout 写不会破坏消息，但会把内容混进 Portal 的输出流——若 Portal 输出结构化日志，混入的裸行会让按行解析的下游失败。框架自身的 Server 日志默认写 stdout，所以生产装配要么给 `runServerProcess` 传一个用 `createJsonLogger({ sink })` 构造、`sink` 写 stderr 的 logger，要么接受两个进程的日志交织在同一 stdout 上并保证下游不假设每行都合法。诊断信息用 `console.error`。

### 一个进程只能有一个 `runServerProcess`

`process.send` 与 `message`/`disconnect` 事件是进程级的，一个进程只有一条到父进程的 IPC 链路。第二次调用 `runServerProcess` 会同步抛出 `a process channel already exists`，而不是让两个实例各自注册监听器、把同一条消息处理两遍——那等于两个存储写者（INV-002）。需要多个 Server 就启动多个 OS 进程，每个 Portal 只拉起一个。

### Portal 的 `timeoutMs` 是权威，Server 只收到剩余预算

Portal 侧 `IpcExecutor` 的 `timeoutMs` 决定请求何时在 Portal 结算；发给 Server 的 `timeoutMs` 是"从发送那一刻起还剩多少预算"，且 Server 只会在自己配置与后端声明之上再收紧，不会放宽。后果：Server 侧 `timeoutMs` 配得比 Portal 大没有意义，配得小则先于 Portal 生效，请求会以 Server 的 `FRAMEWORK_TIMEOUT` 结束。两边配置不一致时，实际生效的总是更小的那个，但"谁先结算"决定日志里出现哪一侧的超时记录，因此部署时应让 Server 的 `timeoutMs` 不小于 Portal 的。

## 部署检查清单

- **Portal 崩溃**：Server 的 IPC 链路断开，`createProcessChannel` 收到 `disconnect`，日志出现 `IPC_CHANNEL_CLOSED`（`reason: process disconnected`）；Server 排空在途请求、关闭后端，`runServerProcess` 以 `ServerChannelClosedError` reject。进程是否随之退出由入口脚本决定——上例把它映射为非零退出码，这是让 Portal 的崩溃处理看见"异常结束"而不是"正常关闭"的必要一步。它不会变成孤儿继续写存储。未确认输入随 Portal 一起丢失，这是 SPEC 6.1 允许的。
- **`stop()` 的语义**：Portal 停止接受输入，排空已接受的输入（受 `ConnectionLayer.shutdownTimeoutMs` 约束）；仍在 Session 屏障前、尚未调用 executor 的请求以 `FRAMEWORK_SHUTDOWN` 结束，不绕过屏障执行。随后执行器关闭 Server：声明 `shutdown` 预算，Server 在该预算内排空并关闭存储、回 `closed`，Portal 再有界等待实际退出，必要时 kill 后再等。退出仍未确认时保留写者屏障并拒绝关闭，`ConnectionLayer` 尽力关闭传输后传播失败，不静默成功。`FRAMEWORK_SHUTDOWN` 本身没有阶段信息：明确未调度的请求未执行，可能已投递而未成功确认的请求则可能已执行或提交，不能仅凭错误码安全重试。外部停止时限必须覆盖连接层排空、执行器关闭、传输关闭三段完整上界，公式见 ADR-0004；`IpcExecutor.closeBudgetMs` 只是中间一段的预算。systemd `TimeoutStopSec` 等应长于完整上界；Server 的请求 `timeoutMs` 不改变 Portal 声明的关闭预算。
- **确认没有孤儿进程**：`process.kill(pid, 0)` 抛 `ESRCH` 可用于检查该 pid 已消失。`pid` 来自 Server 的 `ready` 消息（普通启动会记入 `server ready` 日志）。框架内部只有 `observed: true` 的退出确认才解除写者屏障；等待超时说明尚未确认，不能仅凭日志诊断进程不可中断。退出 Promise 仍 pending 时，迟到确认可在执行器未到终态时恢复一次；一次性 `observed: false` 结果不保证自动恢复，需要部署方处理，不能另起写者绕过。
- **`maxPending`**：Portal 未确认缓冲的硬上限（默认值见 ADR-0004），超出后请求以 `FRAMEWORK_QUEUE_FULL` 拒绝。deadline 在 Server 未就绪或不可用时仍会结算请求，只限制单条请求的驻留时间；总数上限控制同时在途的数量，不能用来延长请求的 deadline。
- **`maxConnections`**：连接层同时接纳的活跃连接上限（默认 10000，见 SPEC 6.1）。达到上限后，新连接以 WebSocket close code `1013`（Try Again Later）关闭，不进入请求处理；连接断开才释放名额。它是准入上限，不是业务 Session 的生命周期限制。

## 常见错误

**入口脚本路径写错**：子进程以 `Module not found` 退出（退出码 1，错误在 stderr），Portal 看到的是 `ipc channel disconnected` 加一次 `server process lost`。随后按退避重试，直到 `startTimeoutMs`（默认 `max(10s, restartBackoffMs * 10)`）耗尽，`start()` reject 并停止重试。表现为"启动很慢然后失败"，不是立即失败；缩短 `startTimeoutMs` 可以让配置错误更快暴露。

**Server 起来就崩**：退避从短间隔起倍增到上限，重置条件是稳定运行超过一个远大于退避上限与请求超时的窗口（默认值见 ADR-0004）。所以"起来、接第一个请求、崩"会持续计入同一条崩溃链，不会退回零退避。同一条请求的重放有上限（`maxReplays`，默认值见 ADR-0004），耗尽后以 `FRAMEWORK_REPLAY_EXHAUSTED` 结束。初次 `start()` 仍有覆盖重试的总体 `startTimeoutMs`；自动恢复的每一代另有独立握手时限，旧代计时器不影响新代。请求的 deadline 不因重启而延长，退出未确认时也不会为恢复服务而绕过唯一写者屏障。

**Portal 里直接开了存储**：不会立刻报错，但恢复语义失效——Portal 崩溃时已写入而 Server 未确认的部分无法重放也无法回滚（INV-002）。存储只能出现在 Server 的 `createBackend` 里。

**用 Node 启动 Server**：没有错误，只有 `start()` 一直等到 `startTimeoutMs` 才失败。这是最容易误判为"启动慢"的配置错误。
