# 双进程装配

框架分成两个进程（ADR-0002）：Portal 持有连接，Server 执行 Handler 并独占存储写入。框架提供两端的函数，不提供进程管理器，两个进程必须由使用者自己装配。本文只写装配决策与边界；协议与恢复语义见 ADR-0004。

本文是操作文档，不是契约：可复制的代码与部署检查清单属于这里，承诺与不变量属于 SPEC 与 INVARIANTS。与它们冲突时以它们为准。框架、消费方与部署方的职责分工见 `docs/RESPONSIBILITY.md`；本文只写装配与部署操作。

## 最小可运行装配

### Portal

```ts
import { WsTransport } from 'mud-framework-connector-ws';
import {
  ConnectionLayer,
  IpcExecutor,
  spawnBunServer,
} from 'mud-framework-core';

const requestTimeoutMs = Number(process.env.REQUEST_TIMEOUT_MS ?? 5_000); // 示例值，按部署调整

const executor = new IpcExecutor({
  spawn: () => spawnBunServer({ command: ['bun', 'run', './server/main.ts'] }),
  timeoutMs: requestTimeoutMs,
});
const layer = new ConnectionLayer({
  transport: new WsTransport({ port: Number(process.env.PORTAL_PORT) }),
  executor,
});

await layer.start();

process.once('SIGTERM', () => {
  void layer.stop().then(() => process.exit(0));
});
```

Portal 不碰存储：这里没有任何 backend。`layer.start()` 会先拉起 Server，等它回 `prepared` 后自动发 `activate`，再等到 `ready` 才开始接受连接（SPEC 6.2，两阶段见 ADR-0007）。端口从部署环境读取；`timeoutMs` 必填，示例给了可运行的缺省值，取值语义见 `IpcExecutorOptions`，按部署调整。

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
              const { name } = payload as { name: string };
              await ctx.storage.execute(
                'INSERT INTO players (name) VALUES (?)',
                [name],
              );
              return { created: name };
            },
            { access: 'public' },
          );
        },
      },
    ],
    timeoutMs: Number(process.env.REQUEST_TIMEOUT_MS ?? 5_000),
  });
} catch (error) {
  // 启动或链路失败映射为非零退出，供进程管理与诊断识别。
  console.error(
    'server stopped abnormally:',
    error instanceof Error ? error.message : String(error),
  );
  process.exitCode = 1;
}
```

片段的前置条件是 `./data` 目录已存在，且数据库已有业务表 `players (name TEXT NOT NULL)`。`SqliteBackend.open` 既不创建父目录，也不创建业务 schema；目录缺失会导致打开失败，表缺失会让 `player.create` 返回 `INTERNAL_ERROR`。schema 初始化与迁移属于应用，必须在接受业务请求前完成；可放在 `createBackend` 中，但不得提前到 Portal 或 prepared 阶段。`examples/minimal/` 是可直接运行的版本，会在 `createBackend` 中建目录并用 `CREATE TABLE IF NOT EXISTS` 建表。

## 访问声明与登录装配

Handler 默认 `access: 'session'`：请求没有 Session 时以 `AUTH_REQUIRED` 拒绝，且不会打开事务或执行 Handler。登录/注册这类入口声明 `access: 'public'` 并加 `mayChangeSession: true`，在 handler 内校验凭据后调用 `ctx.bindSession(id)`；绑定只在请求成功后生效。退出登录同理：public + `mayChangeSession: true` 的 handler 调 `ctx.unbindSession()`，成功后路由被移除，失败时保留。

凭据校验、用户数据、Session ID 的生成与保留策略属于消费方，框架不提供用户库或登录协议。需要更细的授权判断时，把 `authorize(ctx, method, payload)` 传给 `runServerProcess`：它在 Session 检查之后、事务之前执行，只看到请求身份、`AbortSignal` 与日志器；返回 false 得到 `FORBIDDEN`，抛错按 `INTERNAL_ERROR` 处理，决策受请求 deadline 约束。

`runServerProcess` 返回的 Promise 就是退出信号：收到 `shutdown` 并正常收尾时 resolve，启动失败或链路断开时 reject。入口脚本应把失败映射为非零退出码；Portal 也通过 IPC 断开与退出事件发现 Server 丢失。

## Server 主动事件

Handler 可以在请求内声明事件，框架在请求成功提交后发送（ADR-0008）。`ctx.emit` 发给当前请求的连接，`ctx.emitToSession` 发给该 Session 当前的持有连接：

```ts
registrar.register('room.enter', async (ctx, payload) => {
  const { roomId, sessionIds } = payload as {
    roomId: string;
    sessionIds: readonly string[];
  };
  // 成员与可见性由业务校验：框架不做事件授权。
  for (const sessionId of sessionIds) {
    ctx.emitToSession(sessionId, 'room.entered', { roomId });
  }
  ctx.emit('room.joined', { roomId });
  return { roomId };
});
```

事件在 handler 内只进入缓冲：请求失败、超时、取消或回滚时全部丢弃，成功提交后才在响应之后发送。目标 Session 在投递时解析当前持有者，所以重连后事件会跟随 Session；当时没有持有者则丢弃。投递是 best-effort，不排队、不重试、不保证不重复；事件失败不会改变请求结果。

客户端收到两类下行帧。响应帧保持 `{id, ok, ...}`，事件帧是 `{kind:'event', eventId, event, payload}`：

```ts
socket.addEventListener('message', (event) => {
  const frame = JSON.parse(event.data) as Record<string, unknown>;
  if (frame.kind === 'event') {
    // eventId 为 `${requestId}:${index}`；重放可能重复，可据此去重。
    handleServerEvent(frame);
    return;
  }
  handleResponse(frame);
});
```

事件帧不携带请求的 `id`，也不替代响应：同一个请求先到响应、再到事件。客户端不得因为不认识的事件名或事件帧而断开连接。

## 健康探针（应用层）

core 只提供同步快照，不提供 HTTP 端点（SPEC 6.6）。应用在自己的 HTTP 服务里把 `layer.health()` 映射为探针：`ready === true` 返回 200，否则 503；需要区分原因时把 `state` 与 `executor.state` 写进响应。快照会立即过期，只表达“此刻可以接活”，不能替代请求准入或 deadline。端点属于应用，`examples/minimal/src/portal.ts` 给出了完整实现。

## 入口选择（应用层）

应用通过 `spawn` 闭包选择入口，框架不感知版本：`IpcExecutor` 与 `runServerProcess` 都不接受、不转发、不记录任何版本标识。发布策略（蓝绿、热修、回滚）由应用决定；`switchTo({ spawn })` 只编排进程替换，不解释入口之间的兼容性。替换顺序与失败边界见 SPEC 6.2 与 ADR-0007。

## 硬约束

**子进程必须是 Bun 进程。** `spawnBunServer` 使用 `Bun.spawn` 的 `ipc` 选项，其序列化格式只与其他 Bun 进程兼容。用 `node` 启动 Server 不会报错：`process.send` 存在，消息被写入，但 Portal 收不到 IPC 回调，表现为启动一直等到 `startTimeoutMs` 才失败。`command` 的第一个元素按 `PATH` 解析，部署环境里 `bun` 必须在 `PATH` 上。

**子进程不得往 stdout 写。** `spawnBunServer` 让子进程的 stdout/stderr 继承 Portal 的对应流（stdin 被忽略），IPC 走独立通道。往 stdout 写不会破坏消息，但会把裸行混进 Portal 的输出流；若下游按行解析结构化日志，混入的裸行会让解析失败。框架自身的 Server 日志默认写 stdout，生产装配要么给 `runServerProcess` 传一个 `sink` 写 stderr 的 logger，要么保证下游不假设每行都合法。诊断信息用 `console.error`。

**一个进程只能有一个 `runServerProcess`。** `process.send` 与 `message`/`disconnect` 事件是进程级的，一个进程只有一条到父进程的 IPC 链路。第二次调用会同步抛出 `a process channel already exists`，而不是让两个实例把同一条消息处理两遍——那等于两个存储写者（INV-002）。需要多个 Server 就启动多个 OS 进程，每个 Portal 只拉起一个。

**Portal 的 `timeoutMs` 是权威。** Portal 侧 `IpcExecutor` 的 `timeoutMs` 决定请求何时在 Portal 结算；发给 Server 的 `timeoutMs` 是“从发送那一刻起还剩多少预算”，Server 只会在自身配置与后端声明之上再收紧，不会放宽。Server 侧配得比 Portal 大没有意义，配得小则先于 Portal 生效；部署时应让 Server 的 `timeoutMs` 不小于 Portal 的。

## 部署检查清单

- **TLS 终止**：不得把明文 WS 直接暴露到不可信网络。部署方必须在不可信网络前终止 TLS（对外 WSS）：推荐由反向代理前置终止，或由具体传输实现自行终止。core 与 `WsTransport` 都不承担 TLS，`WsTransport` 没有 `tls` 选项；TLS 落点见 SPEC 5.2。
- **回环转发**：反向代理转发到 `WsTransport` 时指向 `127.0.0.1`；健康端点同样只放在受保护网络或 TLS 之后，不要单独暴露明文端口。
- **TLS 不是鉴权**：TLS 只保护链路，访问检查与 `authorize` 策略仍由消费方注入（SPEC 3.7）。终止 TLS 不改变 `AUTH_REQUIRED` / `FORBIDDEN` 的语义，也不改变 Envelope 线格式（SPEC 5.4）。
- **Portal 崩溃**：Server 的 IPC 链路断开，日志出现 `IPC_CHANNEL_CLOSED`（`reason: process disconnected`）；Server 排空在途请求、关闭后端，`runServerProcess` 以 `ServerChannelClosedError` reject。入口脚本应把它映射为非零退出码，让 Portal 看见“异常结束”而不是“正常关闭”。未确认输入随 Portal 一起丢失，这是 SPEC 6.1 允许的。
- **`stop()` 的语义**：Portal 停止接受输入，排空已接受的输入；仍在 Session 屏障前、尚未调用 executor 的请求以 `FRAMEWORK_SHUTDOWN` 结束。随后执行器关闭 Server：声明 `shutdown` 预算，Server 在该预算内排空并关闭存储、回 `closed`，Portal 再有界等待实际退出，必要时 kill 后再等。退出仍未确认时保留写者屏障并拒绝关闭，`ConnectionLayer` 尽力关闭传输后传播失败。`FRAMEWORK_SHUTDOWN` 本身没有阶段信息，不能仅凭它安全重试。外部停止时限必须覆盖连接层排空、执行器关闭、传输关闭三段完整上界，公式见 ADR-0004；`IpcExecutor.closeBudgetMs` 只是中间一段的预算。
- **确认没有孤儿进程**：`process.kill(pid, 0)` 抛 `ESRCH` 可用于检查该 pid 已消失。`pid` 来自 Server 的 `ready` 消息。框架内部只有 `observed: true` 的退出确认才解除写者屏障；等待超时说明尚未确认，不能仅凭日志诊断进程不可中断。迟到确认只允许在执行器尚可恢复时解除屏障；一次性 `observed: false` 不保证自动恢复，需要部署方处理。
- **`maxPending`**：Portal 未确认缓冲的硬上限（默认值见 `IpcExecutorOptions`），超出后请求以 `FRAMEWORK_QUEUE_FULL` 拒绝。deadline 在 Server 未就绪或不可用时仍会结算请求，只限制单条请求的驻留时间；总数上限控制同时在途的数量，不能用来延长 deadline。
- **`maxConnections`**：连接层同时接纳的活跃连接上限（默认值见 `ConnectionLayerOptions`）。达到上限后，新连接以 WebSocket close code `1013`（Try Again Later）关闭，不进入请求处理；连接断开才释放名额。它是准入上限，不是业务 Session 的生命周期限制。

### TLS 终止配置要点

反向代理终止 TLS 只需要三件事：对外只监听 TLS 端口、转发 WebSocket upgrade 头、后端只监听回环地址。把下列配置中的 `<portal-port>` 换成 Portal 实际监听端口。

- nginx：`listen 443 ssl;` 配 `ssl_certificate` / `ssl_certificate_key`；`location /` 使用 `proxy_http_version 1.1;`、`proxy_set_header Upgrade $http_upgrade;`、`proxy_set_header Connection "upgrade";`，转发到 `http://127.0.0.1:<portal-port>`。
- Caddy：`example.com { reverse_proxy 127.0.0.1:<portal-port> }`；证书申请与续期由 Caddy 负责。

具体传输实现自行终止 TLS 时，证书、密钥与监听暴露面由该实现负责；core 不读取任何 TLS 配置。

## 常见错误

| 现象                                                                         | 原因与处理                                                                                                          |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| 启动很慢然后失败，日志有 `ipc channel disconnected` 与 `server process lost` | 入口脚本路径写错，子进程以 `Module not found` 退出。检查 `spawn` 的 `command`。                                     |
| Server 反复重启，同一条请求反复失败                                          | Server 起来后崩溃，退避在稳定窗口前不会重置。修掉崩溃原因；重放预算耗尽后请求以 `FRAMEWORK_REPLAY_EXHAUSTED` 结束。 |
| 恢复语义失效，Portal 崩溃后已写入数据无法重放                                | Portal 里直接开了存储。存储只能出现在 Server 的 `createBackend` 里（INV-002）。                                     |
| `start()` 一直等到 `startTimeoutMs` 才失败                                   | 用 Node 启动了 Server。IPC 只在 Bun 进程之间兼容。                                                                  |
