# 游戏切片示例

一个可运行、可演示的真实 MUD 形状切片：房间、移动、拾取/丢弃、同房间广播、登录/登出与同名重连接管。装配是双进程的：Portal 持有 WebSocket 连接并 spawn Server，Server 是唯一的 SQLite 写者。

它验证的是框架在真实形状下够不够用，不是玩具 echo：所有状态在 SQLite，所有广播走 `ctx.emitToSession`，客户端不轮询。

## 这不是认证方案

`game.login {name}` 是 `public` Handler，只校验名字非空，然后绑定稳定 Session `player:<name>`。任何人可以用任意名字登录，也可以再次用同名登录接管已有 Session。它演示 Session 绑定、重连与 takeover 的语义，不是认证：生产环境必须在登录入口校验凭据，并由真实用户库决定 Session id。

## 前置条件

在仓库根执行过 `bun install`（workspace 包需要按包名解析），Bun 版本见 `.bun-version`。

## 运行

终端一，启动 Portal（`PORTAL_PORT=0` 让操作系统分配端口）：

```sh
GAME_SLICE_DATA_DIR=/tmp/game-slice-data PORTAL_PORT=0 \
  bun run examples/game-slice/src/portal.ts
```

Portal 会打印实际监听地址，例如：

```
portal: listening on ws://127.0.0.1:<port>/
```

终端二，跑脚本化双人演示（它不会自己启动 Portal）：

```sh
GAME_SLICE_URL=ws://127.0.0.1:<port>/ bun run examples/game-slice/src/demo.ts
```

或者交互式客户端，用 `--name` 或 `PLAYER_NAME` 指定玩家名：

```sh
GAME_SLICE_URL=ws://127.0.0.1:<port>/ \
  bun run examples/game-slice/src/client.ts --name alice
```

不设 `PORTAL_PORT` 时使用 `src/portal.ts` 的默认值；此时客户端和 demo 可以只设 `GAME_SLICE_URL`，或者都不设、直接用 `PORTAL_PORT` 指向同一个端口：

```sh
PORTAL_PORT=9090 bun run examples/game-slice/src/portal.ts
PORTAL_PORT=9090 bun run examples/game-slice/src/client.ts --name alice
```

`Ctrl+C` 或 `SIGTERM` 停止 Portal：连接层按自身关闭预算排空在途请求、关闭 Server 并停止监听，正常关闭退出码为 0。Portal 退出后 Server 也会退出；如果 Portal 被 `SIGKILL`，Server 通过 IPC 断开与父进程存活轮询发现并自行停止。

## 命令表

交互式客户端读 stdin，一行一个命令：

| 命令             | Handler          | 说明                                            |
| ---------------- | ---------------- | ----------------------------------------------- |
| `look` / `l`     | `game.look`      | 当前房间名、描述、出口、地上物品、同房间玩家    |
| `who`            | `game.look`      | 只打印同房间其他玩家                            |
| `go <direction>` | `game.move`      | 走一个出口；向旧/新房间的其他玩家广播           |
| `say <text>`     | `game.say`       | 向同房间其他玩家广播；文本非空且不超过 500 字符 |
| `take <item>`    | `game.take`      | 拾取当前房间地上的物品；id 或名字均可           |
| `drop <item>`    | `game.drop`      | 丢下背包里的物品到当前房间                      |
| `inv`            | `game.inventory` | 打印背包                                        |
| `quit` / `exit`  | `game.logout`    | 解绑 Session 并关闭连接                         |

Handler 契约：

| Handler          | access    | mayChangeSession | 返回                                                 |
| ---------------- | --------- | ---------------- | ---------------------------------------------------- |
| `game.login`     | `public`  | `true`           | `{sessionId, room}`                                  |
| `game.logout`    | `session` | `true`           | `{loggedOut: boolean, superseded?: true}`            |
| `game.look`      | `session` | 否               | `{room}`                                             |
| `game.move`      | `session` | 否               | `{moved: true, room}`；重放时多一个 `replayed: true` |
| `game.say`       | `session` | 否               | `{said: true}`                                       |
| `game.take`      | `session` | 否               | `{item, alreadyOwned}`                               |
| `game.drop`      | `session` | 否               | `{item, alreadyHere}`                                |
| `game.inventory` | `session` | 否               | `{items}`                                            |

`room` 快照字段：`id`、`name`、`description`、`exits`（方向数组）、`items`（地上物品 `{id,name}`）、`players`（同房间其他在线玩家名）、`you`。

业务错误通过 `BusinessError` 返回，客户端可读；不使用框架错误码：

| code             | 触发场景                          |
| ---------------- | --------------------------------- |
| `BAD_REQUEST`    | 名字/方向/文本/物品参数缺失或越界 |
| `UNKNOWN_PLAYER` | Session 已绑定但找不到对应玩家行  |
| `NO_SUCH_ROOM`   | 玩家所在房间行缺失（数据损坏）    |
| `NO_EXIT`        | 当前房间没有该方向                |
| `NO_SUCH_ITEM`   | 世界中没有这个 id/名字            |
| `ITEM_TAKEN`     | 物品已被其他玩家持有              |
| `ITEM_NOT_HERE`  | 物品在别的房间                    |
| `NOT_OWNED`      | 想丢的物品不在自己背包            |

## 事件语义

服务端主动事件只走 `ctx.emitToSession`，没有轮询接口：

| 事件           | 接收者                       | payload                           |
| -------------- | ---------------------------- | --------------------------------- |
| `room.say`     | 同房间其他在线玩家           | `{from, text}`                    |
| `room.left`    | 旧房间其他在线玩家           | `{player, direction, from, to}`   |
| `room.entered` | 新房间其他在线玩家           | `{player, direction, from, to}`   |
| `item.taken`   | 拾取者所在房间的其他在线玩家 | `{player, item: {id,name}, room}` |
| `item.dropped` | 丢弃者所在房间的其他在线玩家 | `{player, item: {id,name}, room}` |

投递语义（ADR-0008）：

- 同一请求先收到响应，再收到该请求缓冲的事件；同一连接内事件顺序稳定，不同连接之间没有全局顺序。
- 事件在请求提交后才发送；handler 抛错、超时、取消或事务回滚时，事件全部丢弃。
- 投递是 **best-effort**：目标 Session 当时没有连接（离线）、连接已关闭、编码或写失败，事件被丢弃并记 warn，不会重试或补发；请求重放可能让同一事件以相同 `eventId` 重复发送。离线玩家收不到任何事件。
- `eventId` 形如 `${requestId}:${index}`，同一请求的重放会得到相同 id；客户端可以用它去重。框架不保证事件不重复。
- 客户端必须忽略不认识的事件名/帧，不能因为一个事件帧断线。`client-lib.ts` 已按 `{kind:'event'}` 与响应帧分流。

## 世界与种子

Server 启动时幂等建表并插入种子（已存在的行不覆盖，所以重启不会重置玩家位置或物品归属）：

- 房间：`square`（起点）、`market`、`smithy`、`tavern`。
- 有向出口：square north→market、square east→tavern、market south→square、market east→smithy、smithy west→market、tavern west→square。
- 物品：`sword`（square）、`bread`（market）、`lantern`（tavern）。

表：

- `rooms(id, name, description)`
- `exits(room_id, direction, to_room)`，主键 `(room_id, direction)`
- `players(name, room_id, session_id, holder_connection_id)`；`session_id` 为空表示已登出，`holder_connection_id` 记录最近一次登录的框架 Connection id，logout 只在 holder 匹配时清理
- `items(id, name, room_id, owner)`；`owner` 非空表示在玩家背包，此时 `room_id` 为 NULL
- `applied_moves(request_id, player, from_room, to_room, direction)`：移动的幂等账本，用框架稳定的 `requestId` 识别重放

## 数据目录

默认 `examples/game-slice/data/game.sqlite`，目录与表由 Server 在 `createBackend` 阶段自动创建，首次运行无需手工准备。该目录已在根 `.gitignore` 忽略。用 `GAME_SLICE_DATA_DIR` 指向别处（测试与上面的临时目录示例都这样做）。清理：

```sh
rm -rf examples/game-slice/data
```

## 持久化、重连与接管

- 玩家位置与背包都在 SQLite。`game.logout` 只在 `holder_connection_id` 等于当前 `ctx.connectionId` 时清 `session_id` 与 holder，不清 `room_id`，也不清物品 `owner`；同名再次登录会回到原房间、拿回原背包。旧连接在途的 logout 若晚于新登录提交，会得到 `{loggedOut: false, superseded: true}`，不会清掉新 holder 的在线标记。
- 同名在新连接登录会绑定同一个 Session `player:<name>`，Portal 把路由从旧连接改到新连接（takeover）。旧连接仍开着，但不再持有该 Session：它之后发 `game.look` 等 session Handler 会得到 `AUTH_REQUIRED`。
- 登录、移动、拾取、丢弃都做了重放保护：登录是 upsert；移动按 `requestId` 记账，重放不再移动但会重发相同 `eventId` 的事件；重复 `take` 返回 `alreadyOwned: true` 且不重复扣物品；重复 `drop` 返回 `alreadyHere: true`。

## 限制与已知缺口

- **无认证**：任意名字可登录、可接管；README 顶部已说明，生产不得照搬。
- **无战斗、NPC、聊天频道、物品属性**：只覆盖房间/移动/背包/广播/Session。
- **事件不保证送达**：离线、断线、慢连接、目标不在线都会丢；没有补发或历史回放。需要可靠通知时，业务必须自己落库并提供拉取接口。
- **重放可能重复**：`game.say` 重放会把同一句话以相同 `eventId` 再广播一次；`take`/`drop` 重放在状态上幂等，但若原请求已提交而事件尚未发出，观察者可能收不到那次事件（best-effort 的代价）。
- **突然断线没有业务回调**：框架不会通知 Extension“某连接断了”。`players.session_id` 会保留到显式 logout 或同名重新登录，所以房间快照可能短暂列出已经掉线的玩家，发给它的事件会被 Portal 以 `EVENT_DROPPED` 丢弃。生产需要自己的在线态超时/心跳清理。
- **同名登录与旧连接 logout 竞态已用 connectionId 守卫**：`game.login` 把本次 `ctx.connectionId` 写入 `holder_connection_id`；`game.logout` 只在 holder 匹配时清理，所以旧连接晚提交的 logout 不会清掉新 holder 的在线标记。生产仍需心跳/超时清理突然掉线，避免 holder 永久占位。
- **单写者**：只有 Server 打开 SQLite；Portal 只持有连接与路由。SQLite 声明 `concurrency: single`，所有事务串行。
- **移动幂等账本不清理**：`applied_moves` 只增不减；示例规模够用，生产需要保留窗口与清理任务。
- **明文 WS、仅回环**：Portal 绑定 `127.0.0.1`，没有 TLS。对外部署必须经反向代理终止 TLS，见 `docs/INTEGRATION.md`。

## 验证

```sh
bun test tests/integration/game-slice.test.ts
bash scripts/ci.sh --skip-install
```

集成测试会真实 spawn Portal 与 Server，使用临时数据目录和随机端口，验证：同名双登录、`room.say` 事件、`room.left` 事件、拾取/背包、同名重连（takeover）后房间与背包仍在，以及旧连接失去 Session。另有一个不依赖时序的 holder 守卫测试：新 holder 写入后，旧 connectionId 的条件清理不生效。测试结束会发送 `SIGTERM` 并确认没有残留 Server 进程。
