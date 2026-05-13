# Changelog

## [Unreleased] - 2026-05-10

### Added
- **パイプライン処理API** (`POST /api/process`)
  - 複数の後処理ステップを1回のリクエストで実行
  - `refine`（整形）と `translate`（翻訳）を自由に組み合わせ可能
  - 各ステップの結果を配列で返却

- **統合後処理タブ**
  - 翻訳タブと整形タブを「後処理」タブに統合
  - 処理モード選択（なし/整形のみ/翻訳のみ/整形→翻訳）
  - 整形の強度設定（最小限/標準/徹底）
  - 翻訳タイミング制御（文字起こし直後/整形後/手動のみ）

- **プリセット機能**
  - 現在の後処理設定をlocalStorageに保存
  - 保存した設定を読み込んで再利用

- **処理フロー可視化**
  - タブ上部に処理フロー表示: `音声入力 → 文字起こし → 後処理 → 出力`
  - 必須/任意ステップを色分けで明示

- **自動後処理の柔軟化**
  - 整形と翻訳の自動実行を個別に設定可能
  - 翻訳タイミングを細かく制御可能

### Changed
- **タブ構成の変更**
  - 変更前: 入力 / 文字起こし / 翻訳 / 整形 / 出力（5タブ）
  - 変更後: 入力 / 文字起こし / 後処理 / 出力（4タブ）

- **UI/UXの改善**
  - 処理の原則（文字起こし）と2次処理（整形・翻訳）を明確に区別
  - 後処理は任意で柔軟に設定可能に

### Technical Details
- Rust: パイプライン処理のための新しいデータ構造（`PipelineStep`, `ProcessPipelineRequest`, `ProcessPipelineResponse`）
- JavaScript: 設定管理、パイプライン構築、自動処理の最適化
- CSS: 処理フロー表示、後処理セクションのスタイリング

### Documentation
- [docs/improvements.md](improvements.md) - 改善内容の詳細レポート
- [docs/api/postprocess-pipeline.md](api/postprocess-pipeline.md) - パイプラインAPI仕様書
- [CLAUDE.md](../CLAUDE.md) - API設計思想の追加

---

## [0.1.0] - Initial Release

### Added
- Tauri 2 + axum ベースのデスクトップアプリケーション
- Whisper による音声文字起こし
- LLM による文章整形
- LLM による翻訳
- 録音機能
- ドラッグ&ドロップによるファイル入力
- 文字起こし履歴管理
- 翻訳履歴管理
- Live Caption 表示
- TXT ファイル書き出し
