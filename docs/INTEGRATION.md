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

Portal 不碰存储：这里没有任何 backend。`layer.start()` 会先拉起 Server 并等到 `ready`，再开始接受连接（SPEC 6.2）。

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
          registrar.register('player.create', async (ctx, payload) => {
            const name = (payload as { name: string }).name;
            await ctx.storage.execute('INSERT INTO players (name) VALUES (?)', [
              name,
            ]);
            return { created: name };
          });
        },
      },
    ],
    timeoutMs: 5000,
  });
} catch (error) {
  // 非零退出码是 Portal 判定"这次启动失败"的唯一依据。
  console.error(
    'server stopped abnormally:',
    error instanceof Error ? error.message : String(error),
  );
  process.exitCode = 1;
}
```

`SqliteBackend.open` 不会创建父目录：示例中的 `./data` 必须预先存在，否则 `bun:sqlite` 以 `unable to open database file` 失败。

`runServerProcess` 返回的 Promise 就是退出信号：收到 `shutdown` 并正常收尾时 resolve，启动失败或链路断开时 reject。入口脚本必须把这个差别映射到退出码，否则 Portal 无法区分"正常关闭"与"崩溃"。

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

- Server 在 `ready` 中回显自己声明的版本。Portal 配置了版本、且 `ready` 带了不同的版本时记 warn（`expectedCodeVersion`、`actualCodeVersion`、`pid`）；它仍然接受这个 Server，不据此拒绝启动或重启。
- Portal 在接受请求时把版本写入 `request.codeVersion`，重放沿用接受时的值，不会替换成当前 Server 的版本。Server 处理 `replayCount > 0` 的请求、且请求版本与自身版本都存在且不同时记 warn（请求标识字段、`requestCodeVersion`、`serverCodeVersion`、`replayCount`）。
- 任一侧未声明版本时不算"不同"，不记 warn；缺失字段是旧对端的正常形态。
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
- **`stop()` 的语义**：Portal 先停止接受输入，再排空已接收的请求（受 `ConnectionLayer.shutdownTimeoutMs` 约束），然后让 `IpcExecutor` 发送 `shutdown`；Server 停止接受、排空在途请求、关闭存储、回 `closed`；Portal 收到 `closed` 后等进程真正退出，超时则 `SIGKILL`。`stop()` resolve 时子进程通常已经退出，但这个保证是有界的：进程若卡在不可中断状态，Portal 等过一次有界的退出等待后记录 `server process did not exit within the wait; it may still be running` 并继续返回（默认值见 ADR-0004）。关闭执行器那一步的预算取 `ConnectionLayer.shutdownTimeoutMs` 与 `IpcExecutor` 声明的关闭预算（`closeBudgetMs`，公式见 ADR-0004）的较大者，所以默认配置下外层不会先于执行器放弃等待。Server 侧服从 Portal 在 `shutdown` 消息中声明的预算，把 Server 的 `timeoutMs` 配得比 Portal 大不再延长关闭时长（只在消息未带声明时才回退到按 `timeoutMs` 推导）。整条关闭路径因此有确定上界（`closeBudgetMs` 加少量余量），可以据此设置 systemd `TimeoutStopSec` 一类的外部强杀时限，并让它长于框架自己的等待，避免外部先杀掉一个正在正常关闭的 Server。
- **确认没有孤儿进程**：`process.kill(pid, 0)` 抛 `ESRCH` 即进程已消失。`pid` 来自 Server 的 `ready` 消息（`IpcExecutor` 会把它记进 `server ready` 日志行的 `pid` 字段）。关闭后如果该 pid 仍存活，先看是否有 `server process did not exit within the wait` 或 `server did not finish shutting down; killing it` 日志——前者说明进程不可中断，后者说明优雅关闭超时后已发 `SIGKILL`。
- **`maxPending`**：Portal 未确认缓冲的硬上限（默认值见 ADR-0004），超出后请求以 `FRAMEWORK_QUEUE_FULL` 拒绝。它兜住的是"Server 长期不可用而客户端持续写入"这一种 deadline 管不住的情况。

## 常见错误

**入口脚本路径写错**：子进程以 `Module not found` 退出（退出码 1，错误在 stderr），Portal 看到的是 `ipc channel disconnected` 加一次 `server process lost`。随后按退避重试，直到 `startTimeoutMs`（默认 `max(10s, restartBackoffMs * 10)`）耗尽，`start()` reject 并停止重试。表现为"启动很慢然后失败"，不是立即失败；缩短 `startTimeoutMs` 可以让配置错误更快暴露。

**Server 起来就崩**：退避从短间隔起倍增到上限，重置条件是稳定运行超过一个远大于退避上限与请求超时的窗口（默认值见 ADR-0004）。所以"起来、接第一个请求、崩"会持续计入同一条崩溃链，不会退回零退避。同一条请求的重放有上限（`maxReplays`，默认值见 ADR-0004），耗尽后以 `FRAMEWORK_REPLAY_EXHAUSTED` 结束，Portal 继续服务其他请求。

**Portal 里直接开了存储**：不会立刻报错，但恢复语义失效——Portal 崩溃时已写入而 Server 未确认的部分无法重放也无法回滚（INV-002）。存储只能出现在 Server 的 `createBackend` 里。

**用 Node 启动 Server**：没有错误，只有 `start()` 一直等到 `startTimeoutMs` 才失败。这是最容易误判为"启动慢"的配置错误。
