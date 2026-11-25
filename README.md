# mud-framework

通用 MUD 游戏框架，基于 Bun + TypeScript。

## 当前状态

工程基础设施已就绪（workspace、格式化与静态检查、类型检查、测试入口、本地与 CI 共用的门禁脚本、Git 钩子、GitHub Actions）。`packages/` 下有四个 workspace 包：core、connector-ws 与 storage-sqlite 实现存储契约、传输契约与端到端链路，test-support（`mud-framework-test-support`）是 private 包，仅用于测试辅助，不参与对外契约；对外契约见 `docs/SPEC.md`。

## 快速开始

需要 Bun >= 1.4.5。`bun install` 安装依赖并启用 Git 钩子，之后用 `bun run test` 跑测试，或用 `bash scripts/ci.sh --skip-install` 跑完整门禁。各命令说明与提交规范见 `docs/CONTRIBUTING.md`。

想先看到双进程跑起来：在仓库根执行 `bun run examples/minimal/src/portal.ts`，另开终端执行 `bun run examples/minimal/src/client.ts`；端口、数据目录与健康检查见 `examples/minimal/README.md`。

## 目录结构

```
mud-framework/
├── .githooks/         pre-commit 与 commit-msg 钩子
├── .github/           CI 配置与 PR 模板
├── benchmarks/        手动性能基准，不接入 CI
├── docs/              规格、不变量与协作规范
├── examples/          最小双进程示例，从仓库根可直接运行
├── packages/          四个包：core、connector-ws、storage-sqlite，以及 private 的 test-support（仅测试辅助）
├── scripts/           门禁脚本，本地与 CI 共用
├── tests/             跨包集成测试
├── .bun-version       Bun 版本约束
├── .gitignore         Git 忽略规则
├── .oxfmtrc.json      格式化配置
├── .oxlintrc.json     静态检查配置
├── bun.lock           依赖锁文件
├── LICENSE            MIT 许可证全文
├── package.json       workspace 与脚本入口
├── README.md          本文件
├── tsconfig.json      TypeScript 配置
└── AGENTS.md          AI 助手行为规范
```

## 文档

- `docs/SPEC.md`：框架的对外契约，含五条承诺、执行模型、存储契约、传输契约与生命周期。
- `docs/INVARIANTS.md`：产品承诺的不变量清单，违反即为 bug。
- `docs/INTEGRATION.md`：双进程装配指南，含最小可运行代码、硬约束与部署检查清单。操作文档，不是契约。
- `docs/CONTRIBUTING.md`：协作流程，含开发环境、门禁命令、提交规范与 PR 流程。

## 许可证

MIT，版权归 `luck` 所有，全文见 `LICENSE`。
