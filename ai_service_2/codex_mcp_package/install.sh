#!/usr/bin/env bash
set -euo pipefail

PACKAGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_NAME="schemist"
MARKETPLACE_NAME="personal"

if ! command -v codex >/dev/null 2>&1; then
  echo "未找到 codex 命令。请先安装或更新 Codex 桌面版。" >&2
  exit 1
fi

codex plugin marketplace add "$PACKAGE_DIR"
codex plugin add "$PLUGIN_NAME@$MARKETPLACE_NAME"

echo "Schemist已安装。请重启 Codex 桌面端，并在新对话中使用。"
