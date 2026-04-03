# Notion連携セットアップ手順（MCP + 差分同期）

この手順は、Notion側が未設定の状態から開始できます。

## 0) 「この親ページだけ」読ませたい場合（推奨）

例：トップに **「投資2026 Mizuki Fund」** のような親ページを1つ作り、その**配下だけ**に日誌・Deep Research・戦略メモを置く構成です。

### 内部インテグレーション（APIトークン・差分同期スクリプト）

**こちらはページ単位で確実に絞れます。**

1. 親ページ「投資2026 Mizuki Fund」を開く
2. 右上 `...` → **Connections**（または共有）から、使っている**内部インテグレーションだけ**を追加する
3. **他のページ・他のDBには、そのインテグレーションを追加しない**（追加したページはAPIから読めるようになります）

Notionの公式ドキュメントでは、**親ページにインテグレーションのアクセスを付与すると、その配下の子ページにもアクセスが及ぶ**旨が説明されています（[Authorization](https://developers.notion.com/docs/authorization)）。  
Mizuki用のコンテンツはすべてこの親の**中にだけ**置くと、意図しないページを共有しにくくなります。

差分同期（[`bi/pipelines/notion_incremental_sync.py`](bi/pipelines/notion_incremental_sync.py)）では、さらに **`NOTION_DATABASE_ID` で1つのDBに固定**するので、取得対象はそのDBの行だけです。

### Cursor の Notion MCP（OAuth）

公式の説明では、接続後は **あなたのNotionアカウントの権限の範囲**で読み書きできる旨が述べられています（ワークスペース全体を見えるメンバーなら、ツールによっては広く検索できる余地があります）。

**「MCPだけ物理的に1親ページにロック」** は、内部インテグレーションほど厳密ではないことがあります。厳しく切りたい場合の現実的な対策は次です。

- **対策A**：Mizuki専用の **別ワークスペース** を作り、その中に「投資2026 Mizuki Fund」ツリーのみ置く（MCPもそのワークスペースだけOAuthする）
- **対策B**：MCPは補助にとどめ、表形式のデータは **差分同期スクリプト＋DB1つ** で絞る（本リポの [`bi/pipelines/notion_incremental_sync.py`](bi/pipelines/notion_incremental_sync.py)）

## 1) Notion側で最初に行うこと

1. Notionで「内部インテグレーション」を作成する  
   - Notionの `Settings` → `Connections`（または `Integrations`）→ `Develop or manage integrations`  
   - 新規インテグレーションを作成し、トークンを発行します
2. 同期したいデータベースを開く
3. データベース右上の `...` → `Connections` から、作成したインテグレーションを追加する
4. データベースURLから `NOTION_DATABASE_ID` を控える  
   - URL末尾付近の32文字（ハイフンありでも可）

## 2) CursorでMCPを設定する（Notionチャット連携）

リポジトリ内にテンプレートがあります:

- `dev/notion_mcp_cursor.example.json`

CursorのMCP設定に以下を追加してください:

```json
{
  "mcpServers": {
    "notion": {
      "url": "https://mcp.notion.com/mcp"
    }
  }
}
```

設定後、Cursorを再起動し、Notion認証が要求されたらブラウザで許可します。

## 3) ローカル差分同期（BI用）を設定する

PowerShellで以下を実行します:

```powershell
cd "C:\Users\mizuk\2026年 investment\Mizuki Fund\bi\pipelines"
powershell -ExecutionPolicy Bypass -File .\setup_notion_sync.ps1
```

対話入力で以下を設定します:

- `NOTION_API_TOKEN`
- `NOTION_DATABASE_ID`
- `NOTION_INITIAL_DAYS`（初回取得日数、既定30）

## 4) 同期を実行する

```powershell
cd "C:\Users\mizuk\2026年 investment\Mizuki Fund\bi\pipelines"
powershell -ExecutionPolicy Bypass -File .\run_notion_incremental_sync.ps1
```

成功時は以下が作成・更新されます:

- `bi/outputs/notion_db_incremental.parquet`
- `bi/outputs/notion_sync_state.json`

## 5) つまずきやすいポイント

- `NOTION_API_TOKEN が未設定`  
  - `.env` に値が入っていない、または空です
- `NOTION_DATABASE_ID が未設定`  
  - DB IDの取得漏れ、または誤ったIDです
- 403/404系エラー  
  - インテグレーションを対象DBに `Connections` で追加していない可能性が高いです
