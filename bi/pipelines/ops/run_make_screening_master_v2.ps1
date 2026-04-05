$ErrorActionPreference = "Stop"

# UTF-8 出力設定（日本語文字化け防止）
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

    # Expect KEY=VALUE format
    $m = [regex]::Match($line, "^(?<k>[^=]+)=(?<v>.*)$")
    if (-not $m.Success) { return }

    $k = $m.Groups["k"].Value.Trim()
    $v = $m.Groups["v"].Value.Trim()
    if ($k -eq "") { return }

    # Strip surrounding quotes if present
    if (($v.StartsWith("'") -and $v.EndsWith("'")) -or ($v.StartsWith('"') -and $v.EndsWith('"'))) {
      $v = $v.Substring(1, $v.Length - 2)
    }

    Set-Item -Path "Env:$k" -Value $v
  }
}

if (-not $env:JQUANTS_API_KEY -or $env:JQUANTS_API_KEY.Length -eq 0) {
  throw "JQUANTS_API_KEY が未設定です。.env に JQUANTS_API_KEY=... を記載してください。"
}

# ------------------------------------------------------------
# 実行時固定設定
# ------------------------------------------------------------
$env:YFINANCE_STATEMENT_FALLBACK = "1"          # yfinance財務補完 ON（thin判定の過去方式）
# GitHub Actions（screening_master.yml）と同じ8週・デフォルト日数。4週/30日だと Excel 側の8列に値が埋まらない週がある。
$env:MARGIN_INTEREST_LOOKBACK_WEEKS = "8"       # 信用買い・売り残の週次列（Long/ShortMargin_WkSeq01-08）
$env:MARGIN_INTEREST_DAY_FALLBACK = "2"          # 信用残: 日付フォールバック2日
# SHORT_SALE_LOOKBACK_DAYS は未設定でよい（Python 既定: 週数×7+14 日）。30日だけだと8週スナップの古い週が欠けやすい。

python .\make_screening_master_v2.py --no-excel

