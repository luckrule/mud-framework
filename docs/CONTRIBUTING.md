# CONTRIBUTING

本文件是参与本仓库必须满足的约束。项目定位见 `README.md`，架构与契约见 `docs/SPEC.md`；代码风格、依赖、strict、注释与文档写作约定见仓库根目录 `AGENTS.md`，本文件不重复。

## 开发环境

Bun 版本以 `.bun-version` 为准，`package.json` 的 `engines` 给出最低要求。执行 `bun install` 安装依赖；`prepare` 会把 `core.hooksPath` 指向 `.githooks`，用了 `--ignore-scripts` 时手动执行 `git config core.hooksPath .githooks`。

## 本地门禁

提交前这些命令必须全部通过：

```bash
bun run fmt:check
bun run lint
bun run typecheck
bun run test
bash scripts/ci.sh --skip-install
```

`pre-commit` 钩子自动执行 `bash scripts/ci.sh --skip-install`，不通过则拒绝提交；CI 调用同一脚本，本地通过即与 CI 一致。

## 测试分层

- `bun run test:unit`：各包测试。core 使用内存假后端与假传输；后端包与传输包使用各自真实实现（后端可用内存数据库，传输可用 loopback）。
- `bun run test:integration`：仓库集成测试，使用真实后端、真实传输与子进程，验证跨进程装配与恢复链路。

框架核心的测试 MUST NOT 依赖真实数据库或真实网络；真实实现只出现在后端包、传输包与仓库级测试中。

## 提交规范

Conventional Commits，格式 `<type>(<scope>): <主题>`，主题用中文。`commit-msg` 钩子会校验。

允许的 `type`：`feat`、`fix`、`docs`、`style`、`refactor`、`perf`、`test`、`build`、`ci`、`chore`、`revert`。

- `scope` 可选，括号内不得为空。
- 破坏性变更 MUST 在 `type` 或 `scope` 后加 `!`，如 `refactor(core)!: ...`。包括公共 API 删除或改名、线格式变化、签名移除或改名；不得用 `docs:` 提交夹带 API 变更。
- `: ` 之后为标题，长度 2–72 字符，首尾不得有空白。
- 以 `Merge `、`Revert `、`fixup! `、`squash! `、`amend! ` 开头的提交信息由钩子放行。

## PR 流程

PR 描述按 `.github/PULL_REQUEST_TEMPLATE.md` 的四节填写：解决什么问题、关联 issue、考虑过的其他方案、需要重点 review 的部分。CI 运行与本地相同的门禁，未通过不合并。
