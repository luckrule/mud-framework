# CONTRIBUTING

本文件是参与本仓库必须满足的约束。项目定位与目录结构见 `README.md`，架构与契约见 `docs/SPEC.md`。

## 开发环境

Bun 版本以 `.bun-version` 为准，`package.json` 的 `engines` 要求 `>=1.4.5`。

```bash
bun install
```

`install` 会执行 `prepare` 脚本，把 `core.hooksPath` 指向 `.githooks`，之后钩子自动生效。若安装时用了 `--ignore-scripts`，需手动启用：

```bash
git config core.hooksPath .githooks
```

## 本地门禁

提交前这些命令必须全部通过：

```bash
bun run fmt:check    # 格式检查
bun run lint         # 静态检查（--deny-warnings）
bun run typecheck    # 类型检查
bun run test         # 测试
bash scripts/ci.sh --skip-install   # 一次跑完以上四项
```

`pre-commit` 钩子会自动执行 `bash scripts/ci.sh --skip-install`，不通过则拒绝提交。CI 调用同一脚本，本地通过即与 CI 一致。

## 提交规范

采用 Conventional Commits，格式 `<type>(<scope>): <主题>`，主题用中文。`commit-msg` 钩子会校验，不符合直接拒绝。

允许的 `type`：`feat`、`fix`、`docs`、`style`、`refactor`、`perf`、`test`、`build`、`ci`、`chore`、`revert`。

- `scope` 可选，括号内不得为空。
- 破坏性变更 MUST 在 `type` 或 `scope` 后加 `!`，如 `refactor(core)!: ...`。破坏性变更包括公共 API 删除或改名、线格式变化、签名移除或改名；不得用 `docs:` 提交夹带 API 变更。
- `: ` 之后为标题，长度 2–72 字符，首尾不得有空白。
- 以 `Merge `、`Revert `、`fixup! `、`squash! `、`amend! ` 开头的提交信息由钩子放行，不校验。

## PR 流程

PR 描述按 `.github/PULL_REQUEST_TEMPLATE.md` 的四节填写：解决什么问题、关联 issue、考虑过的其他方案、需要重点 review 的部分。

CI 会运行与本地相同的门禁，门禁未通过不合并。

## 代码与文档约定

代码风格、依赖、strict、注释与文档写作约定统一见仓库根目录的 `AGENTS.md`，本文件不重复。
