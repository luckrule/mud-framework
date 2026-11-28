# 最小双进程示例

可运行的最小装配：Portal 持有 WebSocket 连接并拉起 Server，Server 独占 SQLite 写入，Handler 演示访问门与 Session 绑定/解绑。

## 前置条件

Bun 版本见 `.bun-version`；在仓库根执行过 `bun install`，workspace 包才能被示例按包名导入。

## 启动（均在仓库根）

终端一启动 Portal：

```sh
bun run examples/minimal/src/portal.ts
```

终端二运行演示客户端：

```sh
bun run examples/minimal/src/client.ts
```

Portal 会自动 spawn `examples/minimal/src/server.ts`，不需要单独启动 Server，也不要直接运行它：Server 依赖 Portal 建立的 IPC 通道。客户端依次发送 `player.login`、`player.whoami`、`player.logout`、`player.whoami`，最后一步得到 `AUTH_REQUIRED`，证明登出后 Session 门重新生效。`Ctrl+C` 或 `SIGTERM` 停止 Portal；正常关闭的退出码为 0。

## 端口

Portal 只绑定 `127.0.0.1`。端口由环境变量覆盖，默认值见 `src/portal.ts` 与 `src/client.ts` 的常量。

| 环境变量      | 用途                               |
| ------------- | ---------------------------------- |
| `PORTAL_PORT` | WebSocket 入口；客户端也读这个变量 |
| `HEALTH_PORT` | 应用层健康端点                     |
| `PLAYER_NAME` | 仅客户端：登录用的名字             |

覆盖端口时，两个进程必须读同一个 `PORTAL_PORT`：

```sh
PORTAL_PORT=<port> HEALTH_PORT=<health-port> bun run examples/minimal/src/portal.ts
PORTAL_PORT=<port> bun run examples/minimal/src/client.ts
```

## 数据目录

`examples/minimal/data/`，数据库文件为 `world.sqlite`。目录与 `players` 表由 Server 在启动阶段自动创建，首次运行无需手工准备；该目录已在仓库根 `.gitignore` 中忽略。清理示例数据：

```sh
rm -rf examples/minimal/data
```

## 健康检查

```sh
curl -i "http://127.0.0.1:<health-port>/"
```

`layer.health().ready` 为 true 时返回 200，否则 503；响应体为 `{status, health}`，`health` 是完整快照。端点由应用层提供，core 只提供同步快照（SPEC 6.6）；快照立即过期，不能替代请求准入或 deadline。把 `<health-port>` 换成 `HEALTH_PORT` 的值；未设置时端口取 `src/portal.ts` 的默认常量。
