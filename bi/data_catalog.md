# データカタログ

<!-- BIが管理する全データの所在と仕様を記録する -->

## リポジトリ上の主要成果物（`bi/outputs/`）

`make_screening_master_v2.py` を `bi/pipelines` から実行したときの既定出力。Git では **除外がデフォルト**だが、以下は **意図的に追跡**（GitHub Actions が master を更新）:

| ファイル | 内容 |
|---------|------|
| `screening_master.parquet` / `.xlsx` | スクリーニング・マスタ（本体） |
| `screening_master_data_gaps.parquet` / `.xlsx` | 財務欠損などのギャップ一覧 |
| `yfinance_audit.parquet` / `.xlsx` | yfinance 補完の監査用 |

その他の `bi/outputs/*.parquet` / `*.xlsx` は原則ローカル・実験用（`.gitignore` 参照）。`notion_sync_state.json` は同期状態のみローカル。

## ローカル作業データ（Git 管理外）

| 領域 | パス | 備考 |
|------|------|------|
| 生データ・universe・processed | `bi/data/raw/`, `bi/data/universe/`, `bi/data/processed/` | API から再取得可能なものはコミットしない |

## データソース（取得元）

| ソース | 主な用途 | パイプライン入口 |
|--------|---------|-----------------|
| J-Quants API | 株価・財務・信用・空売り等 | `make_screening_master_v2.py` |
| yfinance（任意補完） | 財務欠損時のフォールバック | 同上（環境変数で制御） |
| EDINET | 有報深掘り | `deep_dive.py` 等 |

## スクリーニング定義

| 名称 | 条件参照元 | 出力 |
|------|-----------|------|
| PM 基準スクリーニング | `playbook/stock_criteria.md` | `bi/outputs/screening_master*.parquet`（列定義はパイプラインコード参照） |

## データ品質ルール

- 欠損・要注意銘柄: `*_data_gaps.parquet` / Excel で確認
- 鮮度: 平日バッチ（`.github/workflows/screening_master.yml`）または手元 ETL の実行日を基準に判断
