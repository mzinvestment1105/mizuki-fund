# TSE Reporter — 東証株価変動レポートツール

Claude AI + J-Quants API を使った東証株価変動調査ツールです。

## 機能

| 機能 | 説明 |
|------|------|
| **機能1: スクリーンショット解析** | 株価画像から銘柄・変動率を読み取り、変動理由を Web 調査してレポート生成 |
| **機能2: ±10% 銘柄自動検出** | J-Quants API で前日比 ±10% 以上の全銘柄を取得し、変動理由をレポート生成 |

## フォルダ構成

```
tse-reporter/
├── main.py                    # CLI エントリーポイント
├── requirements.txt
├── .env.example               # 環境変数テンプレート
├── src/
│   ├── config.py              # 設定管理
│   ├── screenshot_analyzer.py # 機能1: 画像解析
│   ├── jquants_client.py      # J-Quants API クライアント
│   ├── stock_screener.py      # 機能2: スクリーナー
│   ├── web_researcher.py      # Web 検索による変動理由調査
│   └── report_generator.py    # Markdown / HTML レポート生成
├── reports/                   # 出力レポート（自動生成）
└── screenshots/               # 入力スクリーンショット置き場
```

## セットアップ

```bash
# 依存パッケージのインストール
pip install -r requirements.txt

# 環境変数の設定
cp .env.example .env
# .env を編集して API キーを入力
```

### .env 設定項目

```env
ANTHROPIC_API_KEY=sk-ant-...      # Anthropic API キー（必須）
JQUANTS_EMAIL=your@email.com      # J-Quants メールアドレス（機能2のみ必須）
JQUANTS_PASSWORD=your_password    # J-Quants パスワード（機能2のみ必須）
REPORT_FORMAT=markdown            # 出力形式: markdown または html
REPORT_OUTPUT_DIR=./reports       # レポート出力先
```

## 使い方

### 機能1: スクリーンショット解析

```bash
# 基本
python main.py screenshot screenshots/stock.png

# 出力フォーマット指定
python main.py screenshot screenshots/stock.png --format html

# 出力先を指定
python main.py screenshot screenshots/stock.png -o reports/my_report.md
```

### 機能2: J-Quants スクリーナー

```bash
# 昨営業日の ±10% 銘柄を調査（デフォルト）
python main.py screener

# 日付を指定
python main.py screener --date 2024-01-15

# 閾値を変更（±5% 以上）
python main.py screener --threshold 5

# 調査銘柄数を制限（API コスト節約）
python main.py screener --max-stocks 10

# HTML 形式で出力
python main.py screener --format html
```

## 技術スタック

| 技術 | 用途 |
|------|------|
| Python 3.11+ | 実装言語 |
| [Anthropic SDK](https://github.com/anthropics/anthropic-sdk-python) | 画像解析・Web 検索・レポート生成 |
| Claude Opus 4.6 + Adaptive Thinking | LLM |
| `web_search_20260209` ツール | 変動理由の Web 調査 |
| [J-Quants API](https://jpx-jquants.com/) | 東証全銘柄の日次株価データ |
| Rich | CLI 表示 |

## レポートサンプル

```markdown
# 東証株価変動レポート

**生成日時**: 2024-01-15 09:30:00
**対象日**: 2024-01-14
**集計対象**: 前日比 ±10% 以上の銘柄

---

## 1. トヨタ自動車（7203）

| 項目 | 値 |
|------|-----|
| 証券コード | 7203 |
| 前日比 | **+12.50%** 🔴 |
| 調査日 | 2024-01-14 |

### 変動理由の概要

2024年1月14日、トヨタ自動車の株価が前日比+12.5%と大幅上昇した主な理由は...
（Claude による調査結果）

### 参照ソース

- https://www.nikkei.com/...
- https://finance.yahoo.co.jp/...
```

## 注意事項

- J-Quants API の無料プランでは当日データに制限があります
- Web 検索は Anthropic の `web_search_20260209` サーバーサイドツールを使用します
- 大量の銘柄を調査すると API コストが増加します（`--max-stocks` で制御）
- 本ツールは情報提供目的です。投資判断には使用しないでください。
