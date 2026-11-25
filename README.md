# mud-framework

通用 MUD 游戏框架，基于 Bun + TypeScript。Bun 版本以 `.bun-version` 为准，`package.json` 的 `engines` 给出最低要求。

## 快速开始

```bash
bun install                       # 安装依赖并启用 Git 钩子
bun run test                      # 包测试与仓库集成测试
bash scripts/ci.sh --skip-install # 完整门禁
```

想先看到双进程跑起来：在仓库根执行 `bun run examples/minimal/src/portal.ts`，另开终端执行 `bun run examples/minimal/src/client.ts`。端口、数据目录与健康检查见 `examples/minimal/README.md`；生产装配先过一遍 `docs/INTEGRATION.md` 的部署检查清单，TLS 由反向代理或具体传输实现终止，明文 WS 不直接暴露到不可信网络。

## 目录

- `packages/` — 框架各包：core、connector-ws、storage-sqlite，以及仅测试用的 test-support
- `examples/` — 最小双进程示例与可运行的游戏切片（房间、移动、拾取、同房间广播）
- `tests/` — 跨包集成测试
- `benchmarks/` — logger、游戏热路径与玩家级负载基准，不接入 CI
- `docs/` — 契约、不变量与协作规范
- `scripts/` — 本地与 CI 共用的门禁脚本
- `.githooks/`、`.github/` — 提交钩子与 CI 配置

## 文档

- `docs/SPEC.md`：对外契约，含五条承诺、执行模型、存储契约、传输契约与生命周期。
- `docs/INVARIANTS.md`：产品承诺的不变量清单，违反即为 bug。
- `docs/INTEGRATION.md`：双进程装配指南，含最小可运行代码、硬约束与部署检查清单。操作文档，不是契约。
- `docs/CONTRIBUTING.md`：协作流程，含开发环境、门禁命令、测试分层、提交规范与 PR 流程。

## 许可证

MIT，版权归 `luck` 所有，全文见 `LICENSE`。
