$ErrorActionPreference = "Stop"

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

$envFile = Join-Path $here ".env"

if (-not (Test-Path $envFile)) {
  New-Item -ItemType File -Path $envFile -Force | Out-Null
}

$raw = Get-Content $envFile -Raw -Encoding UTF8

function Set-Or-AppendEnvValue {
  param(
    [string]$Key,
    [string]$Value
  )

  $current = Get-Content $envFile -Raw -Encoding UTF8
  $pattern = "(?m)^" + [regex]::Escape($Key) + "=.*$"
  $replacement = "$Key=$Value"

  if ([regex]::IsMatch($current, $pattern)) {
    $updated = [regex]::Replace($current, $pattern, $replacement)
    Set-Content -Path $envFile -Value $updated -Encoding UTF8
  } else {
    if ($current.Length -gt 0 -and -not $current.EndsWith("`r`n") -and -not $current.EndsWith("`n")) {
      Add-Content -Path $envFile -Value ""
    }
    Add-Content -Path $envFile -Value $replacement
  }
}

Write-Host "== Notion sync setup ==" -ForegroundColor Cyan
Write-Host "このスクリプトは bi/pipelines/.env に Notion設定を保存します。" -ForegroundColor DarkGray
Write-Host ""

$token = Read-Host "NOTION_API_TOKEN を入力（例: ntn_xxx）"
if (-not $token -or $token.Trim().Length -eq 0) {
  throw "NOTION_API_TOKEN が空です。"
}

$dbId = Read-Host "NOTION_DATABASE_ID を入力（32文字のID。ハイフン付きでも可）"
if (-not $dbId -or $dbId.Trim().Length -eq 0) {
  throw "NOTION_DATABASE_ID が空です。"
}

$initialDays = Read-Host "初回取得日数 NOTION_INITIAL_DAYS（既定: 30、Enterで既定）"
if (-not $initialDays -or $initialDays.Trim().Length -eq 0) {
  $initialDays = "30"
}

Set-Or-AppendEnvValue -Key "NOTION_API_TOKEN" -Value $token.Trim()
Set-Or-AppendEnvValue -Key "NOTION_DATABASE_ID" -Value $dbId.Trim()
Set-Or-AppendEnvValue -Key "NOTION_INITIAL_DAYS" -Value $initialDays.Trim()

Write-Host ""
Write-Host ".env を更新しました: $envFile" -ForegroundColor Green
Write-Host "次のコマンドで差分同期を実行できます:" -ForegroundColor DarkGray
Write-Host "  powershell -ExecutionPolicy Bypass -File .\run_notion_incremental_sync.ps1" -ForegroundColor Yellow
