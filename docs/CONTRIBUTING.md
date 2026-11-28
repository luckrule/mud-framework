# CONTRIBUTING

本文件是参与本仓库必须满足的约束。项目定位见 `README.md`，架构与契约见 `docs/SPEC.md`；代码风格、依赖、strict、注释与文档写作约定见仓库根目录 `AGENTS.md`，本文件不重复。

## 开发环境

Bun 版本以 `.bun-version` 为准，`package.json` 的 `engines` 给出最低要求。执行 `bun install` 安装依赖；`prepare` 会把 `core.hooksPath` 指向 `.githooks`，用了 `--ignore-scripts` 时手动执行 `git config core.hooksPath .githooks`。

## 本地门禁

门禁以 `sh scripts/ci.sh --skip-install` 为准：`pre-commit` 钩子自动执行它，CI 调用同一脚本，本地通过即与 CI 一致；各步骤的命令定义见 `package.json`。

框架核心的测试 MUST NOT 依赖真实数据库或真实网络；真实实现只出现在后端包、传输包与仓库级测试中。

## 提交规范

Conventional Commits，格式 `<type>(<scope>): <主题>`，主题用中文。`commit-msg` 钩子校验格式、允许的 `type`、标题长度与放行前缀，规则以 `.githooks/commit-msg` 为准。破坏性变更 MUST 在 `type` 或 `scope` 后加 `!`，包括公共 API 删除或改名、线格式变化、签名移除或改名；`commit-msg` 钩子只校验格式，语义由提交者负责。破坏性变更不得用 `docs:` 提交夹带 API 变更。

## PR 流程

PR 描述以 `.github/PULL_REQUEST_TEMPLATE.md` 为准。CI 运行与本地相同的门禁，未通过不合并。
