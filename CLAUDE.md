# SeeDraft

Tauri 2 + axum で構築するデスクトップアプリケーション。
axum が HTTP サーバーとして HTML/API を提供し、Tauri がそれをデスクトップウィンドウで表示する構成。

## Tech Stack

- **Backend**: Rust, axum 0.8, tokio
- **Desktop**: Tauri 2
- **Build**: cargo, tauri-build
- **OS**: Windows (WSL2 で開発)

## Architecture

1. axum が `127.0.0.1:38713` で HTTP サーバーを起動
2. Tauri の `setup` 内でサーバーをバックグラウンド起動
3. サーバー準備完了後に `WebviewWindow` を生成し `http://localhost:38713/index` を表示
4. ウィンドウ破棄時に graceful shutdown

## Project Structure

```
/
├── CLAUDE.md               # このファイル (プロジェクト概要・ルール)
├── Cargo.toml              # 依存関係 (axum, tokio, tauri)
├── build.rs                # tauri_build::build()
├── tauri.conf.json         # Tauri 設定 (productName: SeeDraft)
├── dist/
│   └── index.html          # Tauri 設定上必要なダミー (実際は axum が配信)
├── src/
│   └── main.rs             # エントリポイント (axum + Tauri 起動)
├── res/
│   └── icon.ico            # Windows アイコン
├── icons/
│   └── icon.png            # ビルド用 PNG アイコン
├── docs/                   # 仕様書
│   ├── screen/             # 画面仕様書
│   ├── api/                # API 仕様書
│   └── database/           # DB 設計書
├── tasks/                  # タスク管理 (Kanban 方式)
│   ├── todo/               # これから着手
│   ├── doing/              # 作業中
│   └── done/               # 完了
└── .claude/
    ├── settings.json       # Claude Code 設定
    └── commands/           # カスタムスラッシュコマンド
        ├── spec_new.md     # /spec_new — 仕様書を新規作成
        ├── spec_do.md      # /spec_do  — 仕様書からタスク作成・実装
        └── spec_review.md  # /spec_review — 仕様と実装のレビュー
```

## Build & Run

```bash
# デスクトップアプリとして起動
cargo run

# サーバーモード (ブラウザで確認)
cargo run -- -s 127.0.0.1

# リリースビルド (インストーラ生成)
cargo tauri build
```

## Vibe Coding ワークフロー

1. `/spec_new` — やりたいことを伝えて仕様書を作成
2. `/spec_do` — 仕様書を指定して実装を開始
3. `/spec_review` — 実装後に仕様との整合性をチェック

タスクは `tasks/` ディレクトリで Kanban 管理する:
- `tasks/todo/` → `tasks/doing/` → `tasks/done/`

## Key Design Decisions

- `frontendDist` は Tauri 設定上必要だが、実際の画面は axum が配信する
- サーバー起動完了を `mpsc::channel` で待ってからウィンドウを開く (白画面防止)
- `ShutdownState` で `oneshot::Sender` を管理し、ウィンドウ破棄時にサーバー停止
- `-s` フラグでサーバー単体モードに切替可能

## Conventions

- Rust edition 2024
- フロントエンドの HTML は axum のハンドラ内で直接返す (テンプレートエンジン不使用)
- エラーは `expect` / `eprintln!` でシンプルに処理
- 仕様書は `docs/` 配下に Markdown で管理する
- 仕様書のステータス: Draft → Review → Approved
- アプリケーション内部で整形・抽出・翻訳・補完などの LLM プロンプトを組み立てる場合、原則としてプロンプト本文は英語で記述する

## API Design Philosophy

### 処理フロー
1. **文字起こし（原則処理）** - 常に実行される基本機能
2. **後処理（2次処理）** - 任意で設定可能な追加処理
   - 整形（フィラー除去、文章整形、スタイル調整）
   - 翻訳（多言語対応、字幕向け最適化）

### パイプラインAPI
- `/api/process` - 複数の後処理を1回のリクエストで実行
- ステップを自由に組み合わせ可能（整形のみ、翻訳のみ、整形→翻訳）
- 各ステップの結果を配列で返却

### UI設計
- 処理フローの可視化: `音声入力 → 文字起こし → 後処理 → 出力`
- 4タブ構成: 入力 / 文字起こし / 後処理 / 出力
- 後処理タブで整形・翻訳を統合管理
- プリセット機能でよく使う設定を保存・読込
