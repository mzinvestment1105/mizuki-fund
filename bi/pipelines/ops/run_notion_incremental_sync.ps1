$ErrorActionPreference = "Stop"

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

$opsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pipelineRoot = Split-Path -Parent $opsDir
Set-Location $pipelineRoot

$envFile = Join-Path $pipelineRoot ".env"
if (Test-Path $envFile) {
  Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "") { return }
    if ($line.StartsWith("#")) { return }

    $m = [regex]::Match($line, "^(?<k>[^=]+)=(?<v>.*)$")
    if (-not $m.Success) { return }

    $k = $m.Groups["k"].Value.Trim()
    $v = $m.Groups["v"].Value.Trim()
    if ($k -eq "") { return }

    if (($v.StartsWith("'") -and $v.EndsWith("'")) -or ($v.StartsWith('"') -and $v.EndsWith('"'))) {
      $v = $v.Substring(1, $v.Length - 2)
    }

    Set-Item -Path "Env:$k" -Value $v
  }
}

if (-not $env:NOTION_API_TOKEN -or $env:NOTION_API_TOKEN.Length -eq 0) {
  throw "NOTION_API_TOKEN が未設定です。bi/pipelines/.env に NOTION_API_TOKEN=secret_... を記載してください。"
}
if (-not $env:NOTION_DATABASE_ID -or $env:NOTION_DATABASE_ID.Length -eq 0) {
  throw "NOTION_DATABASE_ID が未設定です。bi/pipelines/.env に NOTION_DATABASE_ID=... を記載してください。"
}

Write-Host "== Notion incremental sync (single DB) ==" -ForegroundColor Cyan
Write-Host ("time: " + (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"))

python (Join-Path $pipelineRoot "notion_incremental_sync.py")

Write-Host "Done." -ForegroundColor Green
