$ErrorActionPreference = "Stop"

# UTF-8 出力（日本語ログの文字化け防止）
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
Set-Location $repoRoot

Write-Host "== Mizuki Fund daily sync ==" -ForegroundColor Cyan
Write-Host ("cwd: " + (Get-Location).Path)
Write-Host ("time: " + (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"))

# 生成物のローカル変更が pull を邪魔しないように戻す（Excelで開きっぱなしだった等）
$outputs = @(
  "bi/outputs/screening_master.parquet",
  "bi/outputs/screening_master_data_gaps.parquet",
  "bi/outputs/yfinance_audit.parquet",
  "bi/outputs/screening_master.xlsx",
  "bi/outputs/screening_master_data_gaps.xlsx",
  "bi/outputs/yfinance_audit.xlsx"
)

foreach ($p in $outputs) {
  if (Test-Path $p) {
    try {
      git restore $p | Out-Null
    } catch {
      # restore 失敗しても pull で分かるので続行
      Write-Host ("WARN: git restore failed: " + $p) -ForegroundColor Yellow
    }
  }
}

Write-Host "Pulling latest from origin/master..." -ForegroundColor Cyan
git pull --rebase

Write-Host "Converting parquet -> xlsx (if parquet exists)..." -ForegroundColor Cyan
$pairs = @(
  @{ inp = "bi/outputs/screening_master.parquet"; outp = "bi/outputs/screening_master.xlsx" },
  @{ inp = "bi/outputs/screening_master_data_gaps.parquet"; outp = "bi/outputs/screening_master_data_gaps.xlsx" },
  @{ inp = "bi/outputs/yfinance_audit.parquet"; outp = "bi/outputs/yfinance_audit.xlsx" }
)

foreach ($pair in $pairs) {
  $inp = $pair.inp
  $outp = $pair.outp
  if (-not (Test-Path $inp)) {
    Write-Host ("SKIP (missing): " + $inp) -ForegroundColor Yellow
    continue
  }
  try {
    python "bi/pipelines/convert_to_excel.py" --input $inp --output $outp
  } catch {
    Write-Host ("WARN: Excel conversion failed (file open?): " + $outp) -ForegroundColor Yellow
    Write-Host ("  " + $_.Exception.Message) -ForegroundColor Yellow
  }
}

Write-Host "Done." -ForegroundColor Green

