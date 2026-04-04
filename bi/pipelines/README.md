# bi/pipelines

本番のスクリーニング・ETLは **`make_screening_master_v2.py`** と **`convert_to_excel.py`**（リポジトリルート相対・ここをカレントに実行）を主に使います。

| 場所 | 内容 |
|------|------|
| 直下（core） | ETL・ダッシュボード・共有ユーティリティ（`*.py`） |
| `ops/` | 定期実行用 PowerShell・タスクスケジューラ XML |
| `devtools/` | デバッグ用スクリプト・過去版の退避（`archive/`） |
