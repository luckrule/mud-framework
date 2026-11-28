# mud-framework

通用 MUD 游戏框架，基于 Bun + TypeScript。Bun 版本以 `.bun-version` 为准，`package.json` 的 `engines` 给出最低要求。

## 快速开始

```bash
bun install                       # 安装依赖并启用 Git 钩子
bun run test                      # 仓库内全部测试
sh scripts/ci.sh --skip-install   # 完整门禁
```

想先看到双进程跑起来：在仓库根执行 `bun run examples/minimal/src/portal.ts`，另开终端执行 `bun run examples/minimal/src/client.ts`。端口、数据目录与健康检查见 `examples/minimal/README.md`；生产装配先过一遍 `docs/INTEGRATION.md` 的部署检查清单，TLS 由反向代理或具体传输实现终止，明文 WS 不直接暴露到不可信网络。

基准（含 `bench:switch`）不进 CI 门禁，也不做性能断言，见 `benchmarks/README.md`。

## 文档

契约见 `docs/SPEC.md` 与 `docs/INVARIANTS.md`，装配见 `docs/INTEGRATION.md`。参与方式见 `docs/CONTRIBUTING.md`，issue 分诊与状态见 `docs/TRIAGE.md`，安全策略见 `SECURITY.md`。

## 许可证

MIT，版权归 `luck` 所有，全文见 `LICENSE`。
