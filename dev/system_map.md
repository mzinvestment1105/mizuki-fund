# Mizuki Fund — システムマップ

> **目的**: 機能が増えても全体像を把握できる生きたドキュメント。  
> 最終更新: 2026-04-17

---

## Discord チャンネル ↔ スクリプト対応表

| Discord チャンネル | 内容・目的 | Webhook 変数 | 生成スクリプト | 送信スクリプト | 頻度 | 状態 |
|-----------------|-----------|------------|-------------|-------------|------|------|
| `#株式投資etl-daily` | スクリーニングマスターの日次更新完了通知。JQuants から全上場銘柄（~3,800社）の株価・財務・信用残・需給データを取得・整形した parquet/xlsx が更新されたことを知らせる。全パイプラインの基盤データ。 | `DISCORD_WEBHOOK_ETL` | `make_screening_master_v2.py` | GitHub Actions 内蔵 | 平日毎日 | ✅ 自動 |
| `#マクロ環境report-daily` | マクロ経済の日次レポート。RSS（日銀・大和総研・Yahoo）＋ yfinance（日経/S&P/ドル円/金/BTC/VIX）を Anthropic API（Sonnet）で分析。前日レポートとの差分を重視し、変化のないトピックは簡潔化。 | `DISCORD_WEBHOOK_MACRO` | `generate_macro_report.py` | `send_macro_discord.py` | 平日毎日 | ✅ 手動（ローカル実行） |
| `#値動き・売買代金report-daily` | 当日の上昇・下落・売買代金ランキングを統合した1チャンネル。プライム/スタンダード/グロース各上昇5・下落5・売買代金上位を Claude がスイングトレード観点で分析。 | `DISCORD_WEBHOOK_MOVERS` | `make_mover_report.py` | `send_mover_discord.py` | 平日毎日 | ✅ 手動（ローカル実行） |
| `#セクターresport-weekly` | 週次セクター動向レポート。JQuants の OHLCV・投資主体別売買（18セクター）＋ Perplexity 外部 Deep Research の定性情報を Claude が統合分析。来週の地合い判断に使う。 | `DISCORD_WEBHOOK_SECTOR` | `make_sector_raw.py` | `send_sector_discord.py` | 週1（土） | ✅ 手動（ローカル実行） |
| `#銘柄発掘rport-weekly` | 週次投資アイデア候補レポート。全3,800社の TDNet を並列スキャンしてイベント（TOB・上方修正・自社株買い等）を検出 → スコアリング → EDINET DB で財務確認 → Claude がスイング観点で選定・提示。 | `DISCORD_WEBHOOK_IDEAS` | `idea_generator.py` | `send_ideas_discord.py` | 週1（土） | ✅ 手動（ローカル実行） |
| `#個別銘柄report-adhoc` | 個別銘柄の深掘りレポート（随時）。EDINET DB・TDNet・Yahoo 掲示板・ニュース・信用残・空売り残・セクター動向・過去 Perplexity 調査を統合して Claude が分析。投資判断直前の精査に使う。 | `DISCORD_WEBHOOK_RESEARCH` | `deep_dive.py` | `send_report_discord.py` | 随時 | ✅ 手動（ローカル実行） |

---

## 全体データフロー

```
【外部データソース】
  JQuants API              → make_screening_master_v2.py → screening_master.parquet/xlsx
  TDNet（Atom/PDF）        → make_mover_report.py         → _movers_raw.md
                           → idea_generator.py            → _ideas_raw.md
                           → deep_dive.py                 → 適時開示データ付与
  EDINET DB（REST API）    → deep_dive.py / idea_generator → 財務・開示データ付与
  RSS（日銀・大和・Yahoo） → fetch_rss.py                 → _news_raw.md
  yfinance                 → generate_macro_report.py     → 指数・為替スナップショット
  sector_weekly.parquet    → make_sector_raw.py           → _sector_raw.md
                           → idea_generator.py            → セクタースコア補正
                           → deep_dive.py                 → セクター週次コンテキスト
  Perplexity（外部）       → research/stocks/*perplexity*.md → deep_dive.py が自動読み込み
                           → make_sector_raw.py --deep-research-file で明示指定

【Claude 分析（インタラクティブ・ローカル）】
  _movers_raw.md  → Claude → movers/YYYY-MM-DD.md   → send_mover_discord.py  → Discord
  _sector_raw.md  → Claude → sector/YYYY-MM-DD.md   → send_sector_discord.py → Discord
  _ideas_raw.md   → Claude → ideas/YYYY-MM-DD.md    → send_ideas_discord.py  → Discord
  _news_raw.md 等 → Anthropic API → macro/YYYY-MM-DD.md → send_macro_discord.py → Discord
  deep_dive raw   → Claude → レポート               → send_report_discord.py → Discord

【自動実行（GitHub Actions）】
  screening_master.yml : schedule（平日19:00 JST相当）+ workflow_dispatch（手動・外部トリガー可）→ JQuants ETL → push
  macro_report_daily.yml: schedule コメントアウト中（手動 workflow_dispatch は有効）
  sector_report.yml    : schedule コメントアウト中（手動 workflow_dispatch は有効）
```

---

## パイプライン詳細

### 平日毎日

#### スクリーニングマスター（自動）
```
GitHub Actions（`screening_master.yml`・`master` チェックアウト）
  └── make_screening_master_v2.py
        ├── JQuants /fins/summary 集約（`update_statements.py`）
        │     ├── Nx* + 翌期行F* を翌期会計年度で整合
        │     └── NxtFYEn欠損でも最新 EarnForecastRevision の Nx* を救済（先月値残り対策）
        ├── TDNet PDF 幅付き予想フォールバック（`tdnet_forecast_parser.py`）
        └── screening_master.parquet / .xlsx
              └── bi/outputs/ にコミット＆プッシュ → #株式投資etl-daily 通知
```

#### マクロレポート（手動・ローカル実行）
```
python fetch_rss.py --category all
  └── → market/daily/YYYY-MM-DD_news_raw.md

python fetch_finnhub.py [--date YYYY-MM-DD]           ← 任意・推奨
  ├── Finnhub API: /news（general/forex）
  ├── Finnhub API: /calendar/economic（今後7日間・主要国）
  └── → market/daily/YYYY-MM-DD_finnhub_raw.md

python generate_macro_report.py [--date YYYY-MM-DD] [--force]
  ├── _news_raw.md（本日分・日本語RSS）
  ├── _finnhub_raw.md（あれば自動追加・グローバルニュース＋経済カレンダー）
  ├── 前日 macro/YYYY-MM-DD.md（差分コンテキスト・冒頭2500字）
  ├── macro/YYYY-MM-DD_deep_research.md（あれば自動追加）
  ├── yfinance スナップショット（日経/S&P/ドル円/金/BTC/米10年債/VIX）
  ├── Anthropic SDK 直接呼び出し（claude-sonnet-4-6）
  └── → market/daily/macro/YYYY-MM-DD.md

python send_macro_discord.py [--date YYYY-MM-DD]
  └── 本文チャンク送信 + .md ファイル添付 → #マクロ環境report-daily
```
- 新着記事なし → exit code 2 でスキップ＋Discord 通知
- `_finnhub_raw.md` がない場合は従来通り動作（後方互換）
- 他パイプラインと異なり Anthropic SDK を直接使用（ANTHROPIC_API_KEY 必須）
- GitHub Actions の自動スケジュールは停止中。再開時は `macro_report_daily.yml` の schedule を復活

#### 値動き・売買代金レポート（手動・ローカル実行）
```
python make_mover_report.py [--date YYYY-MM-DD]
  ├── TDNet Atom（注目開示タイトルスキャン）
  ├── Yahoo ニュース・掲示板
  ├── screening_master.parquet（銘柄メタ・バリュエーション）
  └── → market/daily/YYYY-MM-DD_movers_raw.md

↓ Claude 分析（インタラクティブ） → market/daily/movers/YYYY-MM-DD.md

python send_mover_discord.py [--date YYYY-MM-DD]
  └── movers/YYYY-MM-DD.md（値動き＋売買代金統合） → #値動き・売買代金report-daily
```

---

### 週次（土曜）

#### セクター週次レポート（手動・ローカル実行）
```
python make_sector_raw.py
  ├── sector_weekly.parquet（JQuants セクター OHLCV・投資主体別売買）
  ├── research/markets/ 最新マクロレポート
  └── → market/daily/YYYY-MM-DD_sector_raw.md
        ※ Deep Research 用プロンプトをターミナルに出力

↓ Perplexity 等で外部 Deep Research（定性調査）を実施

python make_sector_raw.py --deep-research-file <ファイルパス>  # 定性結果を統合して再生成

↓ Claude 分析（インタラクティブ） → market/daily/sector/YYYY-MM-DD.md

python send_sector_discord.py
  └── sector/YYYY-MM-DD.md → #セクターresport-weekly
```

#### 投資アイデア発掘（手動・ローカル実行）
```
python idea_generator.py [--days 7] [--top 30]
  ├── TDNet 全銘柄スキャン（~3,800社・20スレッド並列）
  ├── イベントスコアリング（TOB:10 / 上方修正:9 / 増配:8 / 自社株買い:7 ...）
  ├── 二次フィルタ（時価総額100〜2000億・5日売買代金1億以上）
  ├── セクター週次リターン補正（±1pt）
  ├── EDINET DB 深掘り（上位30社）
  └── → market/daily/YYYY-MM-DD_ideas_raw.md

↓ Claude 分析（インタラクティブ） → market/daily/ideas/YYYY-MM-DD.md

python send_ideas_discord.py [--date YYYY-MM-DD]
  └── ideas/YYYY-MM-DD.md → #銘柄発掘rport-weekly
```

---

### 随時

#### 個別銘柄 Deep Dive（手動・ローカル実行）
```
（任意）Perplexity 等で外部 Deep Research を実施
  └── → research/stocks/XXXX_YYYY-MM-DD_perplexity_*.md として保存

python deep_dive.py --code XXXX
  ├── EDINET DB（企業基本情報・財務時系列・定性テキスト）
  ├── Yahoo 掲示板（個人投資家センチメント・最大30件）
  ├── TDNet 適時開示（直近30日・PDF本文）
  ├── Yahoo Finance ニュース（直近8件）
  ├── screening_master.parquet（バリュエーション・信用残・空売り残）
  ├── sector_weekly.parquet（該当セクター週次動向）
  ├── research/markets/ 最新マクロレポート
  └── research/stocks/ 過去 Deep Dive・Perplexity ファイル（各直近2件・自動読み込み）
        → 全統合 → research/stocks/XXXX_YYYY-MM-DD_data.md

↓ Claude 分析（インタラクティブ） → レポート

python send_report_discord.py
  └── → #個別銘柄report-adhoc
```

---

## GitHub Actions ワークフロー

| ファイル | 本来のスケジュール | 現在の状態 |
|---------|-----------------|-----------|
| `screening_master.yml` | 平日 19:00 JST（Actions `cron` UTC） | ✅ 自動稼働（`schedule` + `workflow_dispatch`）。ETL 時 `NX_FORECAST_RELAXED_REVISION=1` / `TDNET_FORECAST_FALLBACK=1` を明示 |
| `macro_report_daily.yml` | 毎日 18:30 JST | ⏸ schedule 停止中・workflow_dispatch は有効 |
| `sector_report.yml` | 毎週土 20:00 JST | ⏸ schedule 停止中・workflow_dispatch は有効 |
| `sector_gitignore_test.yml` | 手動のみ | 検証用・削除可 |

---

## 外部連携一覧

| サービス | 用途 | 認証方式 | 状態 |
|---------|------|---------|------|
| J-Quants API | 株価・信用残・財務データ | `JQUANTS_API_KEY` | ✅ |
| EDINET DB（edinetdb.jp） | 財務深掘り・開示データ | `EDINETDB_API_KEY` / REST API | ✅ |
| Anthropic API | マクロレポート生成（generate_macro_report.py） | `ANTHROPIC_API_KEY` | ✅（ローカル手動） |
| Finnhub API | グローバルニュース・経済カレンダー（fetch_finnhub.py） | `FINNHUB_API_KEY` | ✅ 設定済み |
| TradingView（screener） | スクリーニング・テクニカル | MCP（Python SDK） | ✅ 設定済み・未テスト |
| TradingView（chart） | チャート操作・Pine Script | MCP（Node.js + CDP） | ✅ 設定済み・未テスト |

> **TradingView Desktop 実行ファイルパス（MSIX インストール）:**
> `C:\Program Files\WindowsApps\TradingView.Desktop_3.0.0.7652_x64__n534cwy3pjxzj\TradingView.exe`
>
 > **CDP付き起動コマンド（チャート連携に必要）:**
> ```powershell
> Start-Process "C:\Program Files\WindowsApps\TradingView.Desktop_3.0.0.7652_x64__n534cwy3pjxzj\TradingView.exe" -ArgumentList "--remote-debugging-port=9222 --remote-allow-origins=*"
> ```
> ※ MSIXアプリのため通常の起動ではCDPが有効にならない。`--remote-allow-origins=*` がないとWebSocket接続が403で弾かれる。
| Notion | 投資データベース | MCP | ✅ 設定済み |
| Discord | レポート通知（7チャンネル） | Webhook | ✅ 全チャンネル設定済み |
| cron-job.org | ETL 外部トリガー | HTTP POST | ✅ |
| Perplexity（外部） | セクター・銘柄の Deep Research | ブラウザ手動 | ✅（ファイル保存で連携） |

---

## ファイル出力マップ

| 出力ファイルパターン | 生成元 | 用途 |
|------------------|-------|------|
| `market/daily/YYYY-MM-DD_news_raw.md` | `fetch_rss.py` | マクロ生成用入力（日本語RSS） |
| `market/daily/YYYY-MM-DD_finnhub_raw.md` | `fetch_finnhub.py` | マクロ生成用入力（グローバルニュース＋経済カレンダー） |
| `market/daily/YYYY-MM-DD_movers_raw.md` | `make_mover_report.py` | Claude 分析用入力 |
| `market/daily/YYYY-MM-DD_sector_raw.md` | `make_sector_raw.py` | Claude 分析用入力 |
| `market/daily/YYYY-MM-DD_ideas_raw.md` | `idea_generator.py` | Claude 分析用入力 |
| `market/daily/macro/YYYY-MM-DD.md` | `generate_macro_report.py`（Anthropic API） | マクロレポート本体（上限5件→archive/） |
| `market/daily/macro/YYYY-MM-DD_deep_research.md` | Perplexity（手動保存） | マクロ Deep Research（上限5件→archive/） |
| `market/daily/movers/YYYY-MM-DD.md` | Claude | 値動き・売買代金統合レポート（上限5件→archive/） |
| `market/daily/sector/YYYY-MM-DD.md` | Claude | セクター週次レポート（上限5件→archive/） |
| `market/daily/sector/YYYY-MM-DD_deep_research.md` | Perplexity（手動保存） | セクター Deep Research（上限5件→archive/） |
| `market/daily/ideas/YYYY-MM-DD.md` | Claude | アイデアレポート（上限5件→archive/） |
| `research/stocks/{コード}/YYYY-MM-DD.md` | Claude | 個別銘柄レポート（上限5件→archive/） |
| `research/stocks/{コード}/archive/` | — | アーカイブ済み個別銘柄レポート |
| `bi/outputs/screening_master.parquet` | `make_screening_master_v2.py` | 全スクリプト共通データ基盤 |
| `bi/outputs/sector_weekly.parquet` | `sector_report.yml` | セクター分析・スコア補正 |
| `bi/outputs/token_usage_log.csv` | `send_mover_discord.py` | トークン消費モニタリング |

---

## 未実装・計画中

| 機能 | 概要 | 優先度 |
|------|------|--------|
| `analyze_movers.py` | Anthropic API 直接呼び出しで動意レポートを全自動化 | 高（予算待ち） |
| マクロ自動生成再開 | `macro_report_daily.yml` の schedule 復活 | 高（予算待ち） |
| PM 投資アイデア FB 機能 | Playbook 基準で候補を構造的評価するプロンプト | 中 |
| TradingView MCP 動作確認 | 日本株（7256.T 等）でテクニカル取得テスト | 低 |
| `idea_generator.py` 財務表示バグ修正 | 売上・利益の単位変換・ROE 小数表示・二次フィルタ | 中 |
