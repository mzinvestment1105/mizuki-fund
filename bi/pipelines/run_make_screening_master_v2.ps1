$ErrorActionPreference = "Stop"

# UTF-8 出力設定（日本語文字化け防止）
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

$envFile = Join-Path $here ".env"
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
$env:YFINANCE_STATEMENT_FALLBACK = "1"          # yfinance財務補完 ON
$env:MARGIN_INTEREST_LOOKBACK_WEEKS = "4"        # 信用残: 直近4週
$env:MARGIN_INTEREST_DAY_FALLBACK = "2"          # 信用残: 日付フォールバック2日
$env:SHORT_SALE_LOOKBACK_DAYS = "30"             # 空売り: 直近30日

python .\make_screening_master_v2.py --no-excel

