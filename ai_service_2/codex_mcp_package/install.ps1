$ErrorActionPreference = "Stop"

$PackageDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PluginName = "schemist"
$MarketplaceName = "personal"

if (-not (Get-Command codex -ErrorAction SilentlyContinue)) {
    throw "未找到 codex 命令。请先安装或更新 Codex 桌面版。"
}

codex plugin marketplace add $PackageDir
if ($LASTEXITCODE -ne 0) {
    throw "注册 Schemist 插件市场失败。"
}
codex plugin add "$PluginName@$MarketplaceName"
if ($LASTEXITCODE -ne 0) {
    throw "安装 Schemist 插件失败。"
}

Write-Host "Schemist已安装。请重启 Codex 桌面端，并在新对话中使用。"
