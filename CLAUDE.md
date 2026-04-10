# Mizuki Fund - AI Hedge Fund Organization

## Overview
Mizuki Fundは、個人スイングトレーダーがAIエージェントチームを活用してヘッジファンドのような体制を実現するプロジェクトである。

## 組織構成
- **PM（Portfolio Manager）**: ユーザー本人。最終意思決定者。
- **秘書** (`agents/secretary.md`): タスク・アイデア管理、感情モニタリング、日次スケジュール提案
- **マクロ経済アナリスト** (`agents/macro_analyst.md`): マクロ経済・市場全体の分析
- **業界＆個別銘柄アナリスト** (`agents/sector_analyst.md`): セクター・個別銘柄の調査
- **トレーダー** (`agents/trader.md`): 売買タイミング・価格のアドバイス
- **開発** (`agents/developer.md`): エージェント・ツールの開発サポート
- **BI** (`agents/bi.md`): データ基盤・ETL・スクリーニング（JQuants API等）

## 判断の原則
全エージェントは `playbook/` 配下のPMの投資哲学・ルールを尊重し、それに反するアドバイスを行わない。

## データ鮮度ルール（情報過多の防止）
各エージェントは原則として直近データのみ参照し、古いデータはarchive/に退避する。

| 対象 | 保持期間 | アーカイブ先 | アーカイブタイミング |
|------|---------|-------------|-------------------|
| context/journal/ | 直近7日 | context/journal/archive/ | 月末に月次サマリー作成後 |
| context/session/ | 直近7日 | 削除 | セッション開始時に7日超を削除 |
| market/daily/ | 直近5日 | market/daily/archive/ | 月末 |
| research/stocks/ | アクティブカバーのみ | research/stocks/archive/ | カバレッジ外になった時 |
| research/sectors/ | 注目業界のみ | research/sectors/archive/ | 対象外になった時 |
| portfolio/trade_log.md | 当年分 | portfolio/archive/YYYY.md | 年末 |

月次サマリーは `context/journal/archive/YYYY-MM_summary.md` に格納。

## セッション終了時の自動アクション

PMが「作業終わる」「終わり」「今日は終わり」「おわり」「終了」等の**終了を示す発言をしたら**、明示的に指示されなくても以下を必ず自動実行する：

1. 当日の会話内容を読み取り、`context/session/YYYY-MM-DD.md` を新規作成または追記する
2. 記載内容：完了タスク・次回持ち越し（チェックボックス形式）・メモ
3. 次回セッション開始時にこのファイルを読んで引き継ぐ

## セッション開始時の自動アクション

PMが「前回のタスク読み取って」「引き継ぎ読んで」「前回どこまでやった」「続きから」等の**引き継ぎを示す発言をしたら**、明示的に指示されなくても以下を必ず自動実行する：

1. `context/session/` 配下の最新ファイルを読み込む
2. 「次回持ち越し」の未完了タスクを優先順位順に提示する
3. 「完了タスク」とメモを要約して前回の文脈を伝える

## スクリーニングマスター Excel 更新手順

PMが「エクセル更新」「Excelに反映」「エクセルに更新」等を指示したら、**必ず以下の手順のみを実行する**。余計な確認・調査・git操作は一切行わない。

```bash
cd bi/pipelines
python convert_to_excel.py
```

これだけ。`convert_to_excel.py` が日本語ヘッダへのリネーム・「予想無し」変換・スクリーニングシート生成・書式設定をすべて行う。

## Notion アクセスルール

Notion MCP を使う際は **`投資2026 Mizuki Fund` データベース（ID: `331ededb-8120-817a-a32b-000b84c05095`）配下のページのみ**アクセスする。
他のデータベース・ページ（「投資 GTD」「投資2025」等）は明示的に指示された場合を除きアクセス禁止。

## ディレクトリ構成
- `playbook/` - PMの投資哲学・戦略・ルール（Single Source of Truth）
- `agents/` - 各エージェントのシステムプロンプト
- `context/` - PMのパーソナル情報（日記・アイデア・プロフィール）
  - `context/session/` - セッションログ（`YYYY-MM-DD.md`）。直近7日のみ保持、古いものはセッション開始時に削除
- `market/` - マクロ経済データ・デイリーレポート
- `research/` - 業界・個別銘柄リサーチ
- `portfolio/` - ポジション・トレード計画・売買記録
- `bi/` - データ基盤（ETL・スクリーニング・データカタログ）
  - `bi/pipelines/` - パイプライン本体。構成は [`bi/pipelines/README.md`](bi/pipelines/README.md)（`ops/`＝定期実行スクリプト、`devtools/`＝デバッグ・旧版退避）
  - `bi/outputs/` - 生成データ。`.gitignore` で多くの派生ファイルは除外しつつ、GitHub Actions が **`screening_master*`** と **`yfinance_audit*`** 等の主要成果物をコミット
  - `bi/data/` - 生データ・universe（`raw/` 等は Git 管理外）
- `dev/` - 開発ロードマップ・ツール・スクリプト
- `templates/` - レポート・分析テンプレート

※ 過去のローカル作業用フォルダ名として `python_investment/` がある場合は **リポジトリ対象外**（`.gitignore`）。同等の処理は `bi/pipelines/`（例: 値幅・ムーバー系スクリプト）に寄せる。
