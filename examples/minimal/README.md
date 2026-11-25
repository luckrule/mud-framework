# 最小双进程示例

一个可以真正跑起来的最小装配：Portal 持有 WebSocket 连接并拉起 Server，Server 独占 SQLite 写入，业务 Handler 只演示访问门与 Session 绑定/解绑。没有用户库、密码或角色。

## 前置条件

- Bun >= 1.4.5。
- 在仓库根目录执行过一次 `bun install`，让 workspace 包可以被示例按包名导入。

## 启动（均在仓库根目录）

终端一，启动 Portal：

```sh
bun run examples/minimal/src/portal.ts
```

终端二，运行演示客户端：

```sh
bun run examples/minimal/src/client.ts
```

Portal 启动时会自动 spawn `examples/minimal/src/server.ts`，不需要单独启动 Server，也不要直接运行它：Server 依赖 Portal 建立的 IPC 通道。

客户端按顺序发送 `player.login`、`player.whoami`、`player.logout`、`player.whoami`，最后一步会得到 `AUTH_REQUIRED`，证明登出后 Session 门重新生效。正常情况下会输出类似：

```
client: connected to ws://127.0.0.1:8080/
client: login -> {"player":"demo","sessionId":"player:demo:..."}
client: whoami -> {"sessionId":"player:demo:...","player":"demo"}
client: logout -> {"loggedOut":true}
client: whoami after logout -> AUTH_REQUIRED: ...
client: sequence completed
```

`Ctrl+C` 或 `SIGTERM` 停止 Portal；它会先停止接受连接、关闭 Server，再关闭健康端点。正常关闭的退出码为 0。

## 端口

Portal 只绑定 `127.0.0.1`。

| 环境变量      | 默认值 | 用途                               |
| ------------- | ------ | ---------------------------------- |
| `PORTAL_PORT` | `8080` | WebSocket 入口；客户端也读这个变量 |
| `HEALTH_PORT` | `8081` | 应用层健康端点                     |
| `PLAYER_NAME` | `demo` | 仅客户端：登录用的名字             |

覆盖示例：

```sh
PORTAL_PORT=9000 HEALTH_PORT=9001 bun run examples/minimal/src/portal.ts
PORTAL_PORT=9000 bun run examples/minimal/src/client.ts
```

## 数据目录

`examples/minimal/data/`，数据库文件为 `world.sqlite`。目录与 `players` 表都由 Server 在启动阶段自动创建（`CREATE TABLE IF NOT EXISTS`），首次运行前不需要手工准备。该目录已在仓库根 `.gitignore` 中忽略。

`player.login` 写入或更新 `players`（以 `name` 为唯一键），同时绑定 Session；`player.whoami` 依赖默认的 `session` 访问门并回查该表；`player.logout` 声明 `access: 'session'` 与 `mayChangeSession: true`，调用 `ctx.unbindSession()`。登录不校验密码，任何非空名字都能进入——这里演示的是装配与访问门，不是认证方案。凭据校验、用户数据与 Session 的保留策略属于应用，框架只要求非空 id。

清理示例数据：

```sh
rm -rf examples/minimal/data
```

## 健康检查

```sh
curl -i http://127.0.0.1:8081/
```

`layer.health().ready` 为 true 时返回 200，否则返回 503；响应体包含完整快照，例如：

```json
{
  "status": "ok",
  "health": {
    "state": "ready",
    "ready": true,
    "listening": true,
    "connections": 0,
    "executor": { "state": "ready", "ready": true }
  }
}
```

端点由应用层用 `Bun.serve` 提供，core 只提供同步快照（SPEC 6.6）。快照会立即过期，只表达“此刻可以接活”，不能替代请求准入或 deadline。Portal 停止后端点随进程一起关闭；启动中的快照为 `503`，因为执行器尚未 `ready`。
