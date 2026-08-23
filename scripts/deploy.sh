#!/usr/bin/env bash
# mmwx-probe 部署脚本(CF Workers)
# 用法: ./scripts/deploy.sh
# ProbeHub Durable Object 由 wrangler.jsonc 自动创建/绑定;
# 运行时变量 MMWX_ORIGIN / PROBE_TOKEN 在 CF 控制台配置。
set -euo pipefail
cd "$(dirname "$0")/.."

npm run build
npx wrangler deploy
