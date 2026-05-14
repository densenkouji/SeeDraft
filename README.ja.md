# SeeDraft

> ローカル完結・プライバシーファーストな Voice-to-Text for Windows。
> 録音、文字起こし、整形、翻訳、ノート接続、ドラフト作成まで、デバイス上の AI で行います。

**Languages:** [English](README.md) · **日本語**

---

## 概要

SeeDraft は、音声や音声ファイルを編集可能なノートへ変換し、プロジェクト単位で整理するデスクトップアプリです。クラウドへデータを送信せず、処理はローカル PC 上で完結します。

- **Whisper 音声認識** を Foundry Local 経由で実行
- **ローカル LLM 後処理** による整形、翻訳、抽出、補完、ドラフト合成
- **SQLite 永続化** によるプロジェクト、ノート、タグ、リンク、ドラフト、翻訳、ライブ字幕セッション管理
- **axum + Tauri 2** による Windows デスクトップアプリ

アプリ自体に API キーは不要です。モデル実行には兄弟ディレクトリに配置した Foundry Local SDK を使用します。

## 主な機能

### 録音と文字起こし

- マイク録音、または `wav`, `mp3`, `m4a`, `webm`, `ogg`, `flac`, `aac`, `opus` などの音声ファイルのドラッグ＆ドロップに対応。
- 「モデル」タブから Whisper モデルを選択できます（`whisper-tiny` から大きな Whisper バリアントまで）。
- モデルのダウンロード進捗を Server-Sent Events で表示し、失敗時はバリアントや実行環境の診断情報も表示します。
- 起動時に選択中の音声認識モデルをウォームアップし、初回文字起こし時のモデルロード待ちを減らします。
- 会議トピック、話者名、専門用語、表記ルールなどを文字起こし用プロンプトとして追加できます。
- 必要に応じてバックエンドで Symphonia による音声正規化を行い、マイク録音はブラウザ側で WAV に変換します。

### 後処理パイプライン

- 文字起こし後に「ノート追加」「整形」「翻訳」「抽出」「カスタム」をクイックトグルで自動実行できます。
- **整形** は意味を変えず、フィラー、明らかな誤認識、句読点、`改行` / `句点` / `new line` / `period` などの音声コマンドを補正します。
- **翻訳** は専門用語や追加指示を指定できます。
- **抽出** はトーン、要点、キーワードを生成します。
- **カスタム後処理** では任意の LLM 指示を順番に実行できます。丁寧語、ビジネス文書、議事録、カジュアル化、要約、箇条書き化のプリセットを備えています。
- 手動でリンクしたノートの抜粋を、整形・翻訳・抽出・補完・カスタム後処理の文脈として注入できます。

### ノート、グラフ、ドラフト

- プロジェクト単位でノートを管理し、SQLite に保存します。
- ノートの検索、編集、削除、並べ替え、タグ付け、親子関係の設定ができます。
- グラフビューでノート同士を手動リンクできます。リンク先ノートは LLM の文脈として利用できます。
- プロジェクト、ノート、タグ、手動リンク、親子関係をグラフで可視化します。
- 複数ノートを選択して、見出し付き連結または LLM による 1 本の記事化でドラフトを作成できます。
- ドラフトは編集、保存、削除、Markdown エクスポートに対応しています。
- ノート編集画面から LLM による本文の続きを生成できます。

### ライブ字幕と翻訳

- トップバーからライブ字幕セッションを開始できます。
- マイク音声を連続バッファし、短い無音を検出して発話単位で文字起こしします。
- セッション中に同時翻訳を有効化できます。
- ライブ字幕セッションは保存、リネーム、閲覧、削除できます。

### 日常利用

- ライト、ダーク、システム連動テーマ。
- 日本語 / 英語 UI。ロケール JSON はユーザーデータディレクトリに展開され、追加言語を配置できます。
- プロジェクト、音声認識モデル、認識言語、クイックトグル、カスタムステップ、後処理モデル、テーマ、ロケール、出力ファイル名、ショートカットを保存します。
- キーボードショートカットに対応し、`Ctrl` / `Alt` のダブルタップ割り当ても可能です。
- 通常フィードバックはトースト、重大なエラーはエラーバナーで表示します。

## アーキテクチャ

```
┌────────────────────────────────────────────────────────┐
│ Tauri 2 WebView                                        │
│ ローカル axum サーバーが HTML / CSS / JS を配信        │
└──────────────────────────┬─────────────────────────────┘
                           │ HTTP on 127.0.0.1
┌──────────────────────────▼─────────────────────────────┐
│ axum 0.8 server                                        │
│  ├─ /api/transcribe              Whisper STT           │
│  ├─ /api/process                 後処理パイプライン    │
│  ├─ /api/refine, /api/translate  単体後処理API         │
│  ├─ /api/analyze, /api/complete  抽出・補完            │
│  ├─ /api/projects, /api/notes    SQLiteノート          │
│  ├─ /api/note-links, /api/graph  グラフ関係            │
│  ├─ /api/drafts                  ドラフト              │
│  ├─ /api/translations            翻訳履歴              │
│  ├─ /api/live/*                  ライブ字幕            │
│  ├─ /api/models/*                モデル管理            │
│  └─ /api/download/events         SSE進捗ストリーム     │
└──────────────────────────┬─────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────┐
│ foundry-local-sdk                                      │
│ ONNX Runtime GenAI / WinML on Windows                  │
└────────────────────────────────────────────────────────┘
```

- デスクトップモードでは Tauri の `setup` 内で axum サーバーを起動し、準備完了後に WebView を開きます。
- 優先ループバックポートは `127.0.0.1:38713` です。使用中の場合は OS が割り当てる空きポートにフォールバックします。
- サーバー単体モードでは `-s` で指定したアドレスに bind します。
- ユーザーデータは OS のデータディレクトリに `seedraft.sqlite` として保存され、ロケール JSON は同じ領域の `locales/` に配置されます。
- 一時音声ファイルはシステムの一時ディレクトリ配下 `seedraft-voice-to-text/` に作成されます。

## 動作要件

- Windows 10/11 x64
- Rust ツールチェイン（edition 2024）
- Microsoft C++ Build Tools / MSVC
- 開発およびインストーラ作成用に `../Foundry-Local/sdk/rust` に配置された Foundry Local SDK
- インストーラ作成時は Tauri CLI

生成したインストーラには SDK のビルド出力から取得した Foundry Local native runtime DLL を同梱するため、配布先 PC で Foundry Local アプリや CLI を別途インストールする必要はありません。

## セットアップ

### 1. Rust のインストール

`rustup` で Rust をインストールし、ツールチェインを確認します。

```powershell
rustc --version
cargo --version
```

### 2. Tauri CLI のインストール

```powershell
cargo install tauri-cli --version "^2.0.0" --locked
cargo tauri --version
```

### 3. Foundry Local を兄弟ディレクトリに配置

```powershell
git clone https://github.com/microsoft/Foundry-Local
git clone https://github.com/densenkouji/SeeDraft.git
cd SeeDraft
```

期待する構成:

```
/
├─ Foundry-Local/
│  └─ sdk/rust/
└─ SeeDraft/
   └─ Cargo.toml
```

### 4. 実行

```powershell
# デスクトップアプリとして起動
cargo run

# ブラウザ確認用のサーバー単体モード
cargo run -- -s 127.0.0.1:38713
```

初回利用時、選択中の音声認識モデルと既定 LLM（`qwen2.5-coder-0.5b`）が未キャッシュであれば Foundry Local 経由でダウンロードされます。SeeDraft は Foundry Local の既定モデルキャッシュを使用し、保存先を上書きしません。選択中の音声認識モデルは起動後に自動ウォームアップされます。

### 5. インストーラの作成

```powershell
cargo tauri build
```

生成物は `target/release/bundle/` 配下に出力されます。
ビルド時に必要な Foundry Local native binary は `native/foundry-local/win-x64` にステージングされ、Tauri の `foundry-local` リソースとして同梱されます。ステージングされた DLL 本体は Git 管理外にしています。
既定の bundle target は NSIS の `.exe` インストーラです。

## 操作早見表

| 操作 | 場所 |
|---|---|
| 録音開始 / 停止 | 画面下部の録音ボタン |
| 音声ファイルの文字起こし | アプリ画面へドラッグ＆ドロップ |
| 現在の本文をノート保存 | `ノートに追加` ボタン |
| 後処理の自動実行 | 録音ボタン横のクイックトグル |
| 音声認識モデルの変更・ダウンロード | 設定 -> モデル |
| 専門用語や文字起こしヒントの追加 | 設定 -> 文字起こし |
| 整形、翻訳、カスタム、リンク文脈、抽出、補完の設定 | 設定 -> 後処理 |
| ノート検索・整理 | 左側のノートサイドバー |
| ノート同士のリンク作成 | グラフツールバー -> リンク作成 |
| ドラフト作成 | ノートを選択 -> ドラフト化 |
| ライブ字幕 | トップバー -> ライブ字幕 |
| ドラフトの書き出し | ドラフト画面 -> Markdown エクスポート |

## ディレクトリ構成

```
/
├─ AGENTS.md               # プロジェクト概要・作業ルール
├─ CLAUDE.md               # Claude向けプロジェクト文脈
├─ Cargo.toml              # Rust依存関係
├─ Cargo.lock
├─ build.rs                # tauri_build::build()
├─ tauri.conf.json         # Tauri設定 (productName: SeeDraft)
├─ dist/
│  └─ index.html           # Tauri frontendDist 用プレースホルダ
├─ src/
│  ├─ main.rs              # axum + Tauri、UI、API、モデルサービス
│  ├─ storage.rs           # SQLite データ層
│  └─ locales/             # バンドル済み ja/en ロケールJSON
├─ res/
│  └─ icon.ico
├─ icons/
│  └─ icon.png
└─ docs/                   # API、DB、画面仕様、変更履歴
```

## データモデル

SQLite テーブルは `src/storage.rs` によって作成・マイグレーションされます。

- `projects`
- `notes`
- `tags`
- `note_tags`
- `note_links`
- `drafts`
- `draft_notes`
- `translations`
- `live_sessions`
- `live_segments`

## 主な設計判断

- **ローカル HTTP アプリシェル。** axum が HTML/API を配信し、Tauri はローカル URL を表示してデスクトップウィンドウを管理します。
- **WebView はサーバー準備後に作成。** HTTP サーバーの bind 成功後にウィンドウを開きます。
- **優先ポート + フォールバック。** `38713` を優先し、使用中なら空きループバックポートへ切り替えます。
- **モデルキャッシュとウォームアップ。** 音声認識モデルと LLM はメモリ上でキャッシュし、選択中の音声認識モデルは起動時にウォームアップ、終了時に unload します。
- **SSE によるダウンロード進捗。** `/api/download/events` でモデルダウンロード状況を UI に配信します。
- **構造化されたローカル保存。** プロジェクト、ノート、リンク、ドラフト、翻訳、ライブ字幕は SQLite に永続化し、追加マイグレーションで更新します。
- **意味保持の整形。** 標準の整形は保守的に誤認識やフィラーを補正します。文体変換や要約はカスタム後処理で扱います。
- **モデルの明示的解放。** プロセス終了前に Foundry Local モデルを unload します。

## ロードマップ

- [ ] ライブストリーミング文字起こしの強化
- [ ] リリースパッケージングの整備
- [ ] ローカルランタイムが対応した場合の macOS / Linux 対応

## ライセンス

MIT.

## 謝辞

- [Microsoft Foundry Local](https://github.com/microsoft/Foundry-Local) — オンデバイスモデル実行基盤
- [Tauri](https://tauri.app/) — デスクトップアプリシェル
- [axum](https://github.com/tokio-rs/axum) — HTTP サーバー
- [symphonia](https://github.com/pdeljanov/Symphonia) — 音声デコード
- [rusqlite](https://github.com/rusqlite/rusqlite) — SQLite バインディング
