#!/bin/sh
# 本地与 CI 共用的门禁脚本：依赖安装、格式、lint、typecheck 与测试。
# 用法：scripts/ci.sh [--skip-install]
set -e
cd "$(dirname "$0")/.."

if [ "$1" != "--skip-install" ]; then
  echo "==> install"
  bun install --frozen-lockfile
fi

echo "==> fmt:check"
bun run fmt:check

echo "==> lint"
bun run lint

echo "==> typecheck"
bun run typecheck

echo "==> test"
bun run test

echo "CI gates passed"
