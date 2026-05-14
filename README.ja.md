<p align="center">
    <img src="images/SeeDraft-icon_128x128.png" alt="SeeDraft logo">
</p>
<p align="center">ローカル完結・プライバシーファーストな Windows 向け Voice-to-Text。録音、文字起こし、整形、翻訳、ノート接続、ドラフト作成まで、デバイス上の AI で行います。</p>

# SeeDraft

**Languages:** [English](README.md) | **日本語**

---

[![SeeDraft メイン画面](images/screen01.png)](https://github.com/densenkouji/SeeDraft)

## 概要

SeeDraft は、音声や音声ファイルを編集可能なノートへ変換し、プロジェクト単位で整理する Windows デスクトップアプリです。クラウドへデータを送信せず、処理はローカル PC 上で完結します。

- **Whisper 音声認識** を Foundry Local 経由で実行
- **ローカル LLM 後処理** による整形、翻訳、抽出、補完、ドラフト合成
- **SQLite 永続化** によるプロジェクト、ノート、タグ、ノートリンク、ドラフト、翻訳、ライブ字幕セッション管理
- **Tauri 2 デスクトップ統合** と axum 0.8 のローカル HTTP サーバー

SeeDraft 自体に API キーは不要です。開発時のモデル実行には、兄弟ディレクトリに配置した Foundry Local SDK を使用します。

## 主な機能

### 録音と文字起こし

- マイク録音、音声ファイルのドラッグ＆ドロップ、Windows のどこからでも使える長押しディクテーションに対応。
- 対応拡張子は `wav`, `mp3`, `m4a`, `mp4`, `webm`, `ogg`, `flac`, `aac`, `opus` です。
- 「モデル」タブから Whisper 音声認識モデルを選択できます。既定モデルは `whisper-tiny` です。
- 会議トピック、話者名、専門用語、表記ルールなどを文字起こし用プロンプトとして追加できます。
- モデルのダウンロード進捗を Server-Sent Events で表示し、失敗時はバリアント、実行環境、互換性の診断情報も表示します。
- 起動後に選択中の音声認識モデルをウォームアップし、初回文字起こし時のモデルロード待ちを減らします。
- 必要に応じてバックエンドで Symphonia による音声正規化を行い、マイク録音はブラウザ側で WAV に変換します。

### 後処理パイプライン

- 文字起こし後に「ノート追加」「整形」「翻訳」「抽出」「カスタム」をクイックトグルで自動実行できます。
- **整形** は意味を変えず、フィラー、明らかな誤認識、句読点、`改行` / `句点` / `new line` / `period` などの音声コマンドを補正します。
- **翻訳** は原文または整形後テキストを対象に、専門用語、翻訳先言語、追加指示を指定できます。
- **抽出** はトーン、要点、キーワードを生成します。
- **補完** はノート編集画面から、既存本文の言語と文体を保った続きを生成します。
- **カスタム後処理** では任意の LLM 指示を順番に実行できます。丁寧語、ビジネス文書、議事録、カジュアル化、短い要約、箇条書き化のプリセットを備えています。
- 手動でリンクしたノートの抜粋を、整形・翻訳・抽出・補完・カスタム後処理の文脈として注入できます。

### ノート、グラフ、ドラフト

- プロジェクト単位でノートを管理し、SQLite に保存します。
- ノートの検索、編集、削除、並べ替え、タグ付け、親子関係の設定ができます。
- グラフビューでノート同士を手動リンクできます。リンク先ノートは LLM の文脈として利用できます。
- プロジェクト、ノート、タグ、手動リンク、親子関係をグラフで可視化します。
- 複数ノートを選択して、見出し付き連結または LLM による 1 本の記事化でドラフトを作成できます。
- ドラフトは編集、保存、削除、Markdown エクスポートに対応しています。

### ライブ字幕

- トップバーまたはショートカットからライブ字幕セッションを開始できます。
- マイク音声を連続バッファし、短い無音を検出して発話単位で文字起こしします。
- セッション中に同時翻訳を有効化できます。
- ライブ字幕セッションは保存、リネーム、閲覧、削除できます。

### デスクトップ統合と日常利用

- システムトレイに対応。メインウィンドウを閉じると非表示になり、トレイメニューから表示または終了できます。
- アプリ表示、ライブ字幕表示、録音トグル、設定表示、ウィンドウを閉じる、終了のショートカットを設定できます。
- グローバルな「押下中だけ録音」は既定で `RightCtrl`。結果はクリップボードへコピー、またはカーソル位置へ挿入できます。
- ライト、ダーク、システム連動テーマ。
- 日本語 / 英語 UI。ロケール JSON は初回起動時に展開され、編集や追加言語の `*.json` 配置に対応します。
- プロジェクト、音声認識モデル、認識言語、マイク、クイックトグル、カスタムステップ、後処理モデル、テーマ、ロケールパス、出力フォルダ、出力ファイル名、ショートカットを保存します。
- 通常フィードバックはトースト、重大なエラーはエラーバナーで表示します。

## アーキテクチャ

```
┌────────────────────────────────────────────────────────┐
│ Tauri 2 WebViews                                      │
│ メイン UI と長押し録音オーバーレイを axum が配信      │
└──────────────────────────┬─────────────────────────────┘
                           │ HTTP on 127.0.0.1
┌──────────────────────────▼─────────────────────────────┐
│ axum 0.8 server                                        │
│  ├─ /, /index, /hold-overlay, /assets/icon.ico         │
│  ├─ /api/transcribe                 Whisper STT        │
│  ├─ /api/process                    後処理パイプライン │
│  ├─ /api/refine, /api/translate     単体 LLM 処理      │
│  ├─ /api/analyze, /api/complete     抽出・補完         │
│  ├─ /api/models/*, /api/download/*  モデル管理         │
│  ├─ /api/app/*, /api/locales        アプリ設定         │
│  ├─ /api/projects, /api/notes       SQLite ノート      │
│  ├─ /api/note-links, /api/graph     グラフ関係         │
│  ├─ /api/drafts                     ドラフト           │
│  ├─ /api/translations               翻訳履歴           │
│  └─ /api/live/*                     ライブ字幕         │
└──────────────────────────┬─────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────┐
│ foundry-local-sdk                                      │
│ ONNX Runtime GenAI / WinML on Windows                  │
└────────────────────────────────────────────────────────┘
```

- デスクトップモードでは Tauri の `setup` 内で axum サーバーを起動し、bind 成功を待ってからメイン WebView を開きます。
- 優先ループバックポートは `127.0.0.1:38713` です。使用中の場合は OS が割り当てる空きポートにフォールバックします。
- サーバー単体モードでは `-s` で指定したアドレスに bind します。ホスト名だけを渡した場合はポート `8000` を使います。
- メインウィンドウは `http://127.0.0.1:<port>/index`、透明な長押し録音オーバーレイは `/hold-overlay` を表示します。
- ユーザーデータは OS のデータディレクトリに `seedraft.sqlite` と `settings.json` として保存されます。
- 既定ロケールファイルは実行ファイル横の `locales/` に展開されます。実行ファイルパスが取得できない場合はアプリデータディレクトリを使います。ロケールディレクトリは設定から変更できます。
- 一時音声ファイルはシステムの一時ディレクトリ配下 `seedraft-voice-to-text/` に作成されます。

## 動作要件

- Windows 10/11 x64
- Rust ツールチェイン（edition 2024）
- Microsoft C++ Build Tools / MSVC
- `../Foundry-Local/sdk/rust` に配置された Foundry Local SDK
- インストーラ作成時は Tauri CLI

生成したインストーラには SDK のビルド出力から取得した Foundry Local native runtime を同梱するため、配布先 PC で Foundry Local アプリや CLI を別途インストールする必要はありません。

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

初回利用時、選択中の音声認識モデルと既定 LLM（`qwen2.5-coder-0.5b`）が未キャッシュであれば Foundry Local 経由でダウンロードされます。SeeDraft は Foundry Local の既定モデルキャッシュを使用し、保存先を上書きしません。

### 5. インストーラの作成

```powershell
cargo tauri build
```

生成物は `target/release/bundle/` 配下に出力されます。ビルド時に必要な Foundry Local native binary は `native/foundry-local/win-x64` にステージングされ、Tauri の `foundry-local` リソースとして同梱されます。ステージングされた DLL 本体は Git 管理外にしています。既定の bundle target は NSIS の `.exe` インストーラです。

## 操作早見表

| 操作 | 場所 |
|---|---|
| 録音開始 / 停止 | 画面下部の録音ボタン、またはショートカット |
| 押下中だけディクテーション | 既定のグローバルショートカット: `RightCtrl` |
| 音声ファイルの文字起こし | アプリ画面へドラッグ＆ドロップ |
| 現在の本文をノート保存 | `ノートに追加` ボタン |
| 後処理の自動実行 | 録音ボタン横のクイックトグル |
| モデルの変更・ダウンロード・削除・テスト | 設定 -> モデル |
| 専門用語や文字起こしヒントの追加 | 設定 -> 文字起こし |
| 整形、翻訳、カスタム、リンク文脈、抽出、補完の設定 | 設定 -> 後処理 |
| 出力フォルダ / ファイル名プレフィックスの設定 | 設定 -> 出力 |
| ショートカットと長押し録音の出力先設定 | 設定 -> ショートカット |
| ノート検索・整理 | 左側のノートサイドバー |
| ノート同士のリンク作成 | グラフツールバー -> リンク作成 |
| ドラフト作成 | ノートを選択 -> ドラフト化 |
| ライブ字幕 | トップバー -> ライブ字幕 |
| ドラフトの書き出し | ドラフト画面 -> Markdown エクスポート |
| トレイから復帰 / 終了 | Windows 通知領域のトレイアイコン |

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
│  ├─ main.rs              # axum ルート、API ハンドラ、モデルサービス、起動処理
│  ├─ desktop.rs           # Tauri トレイ、ショートカット、長押し録音コマンド
│  ├─ settings.rs          # アプリ設定、ロケール展開、フォルダ選択
│  ├─ storage.rs           # SQLite データ層とマイグレーション
│  ├─ views.rs             # 埋め込み HTML / アイコンのルートハンドラ
│  ├─ ui/
│  │  ├─ index.html        # メインの単一ファイルフロントエンド
│  │  └─ hold_overlay.html # 透明な長押し録音オーバーレイ
│  └─ locales/             # バンドル済み ja/en ロケールJSON
├─ native/
│  └─ foundry-local/win-x64/
├─ res/
│  ├─ icon.ico
│  └─ app.rc
├─ icons/
│  └─ icon.png
├─ images/                 # README 用アセット
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

- **ローカル HTTP アプリシェル。** axum が HTML/API を配信し、Tauri はローカル URL の表示とデスクトップ固有機能を担当します。
- **WebView はサーバー準備後に作成。** HTTP サーバーの bind 成功後にウィンドウを開きます。
- **優先ポート + フォールバック。** `38713` を優先し、使用中なら空きループバックポートへ切り替えます。
- **独立したオーバーレイ WebView。** 長押しディクテーションは、カーソル付近に出る透明・クリック透過の Tauri ウィンドウで扱います。
- **トレイ前提の close 動作。** メインウィンドウを閉じてもアプリは非表示で継続し、トレイまたは終了コマンドから graceful shutdown します。
- **モデルキャッシュとウォームアップ。** 音声認識モデルと LLM はメモリ上でキャッシュし、選択中の音声認識モデルは起動時にウォームアップ、終了時に unload します。
- **SSE によるダウンロード進捗。** `/api/download/events` でモデルダウンロード状況を UI に配信します。
- **構造化されたローカル保存。** プロジェクト、ノート、リンク、ドラフト、翻訳、ライブ字幕は SQLite に永続化し、追加マイグレーションで更新します。
- **意味保持の整形。** 標準の整形は保守的に誤認識やフィラーを補正します。文体変換や要約はカスタム後処理で扱います。
- **モデルの明示的解放。** プロセス終了前に Foundry Local モデルを unload します。

## ロードマップ

- [ ] ライブストリーミング文字起こしの強化
- [ ] 自動リリースワークフローと署名
- [ ] ローカルランタイムが対応した場合の macOS / Linux 対応

## ライセンス

MIT.

## 謝辞

- [Microsoft Foundry Local](https://github.com/microsoft/Foundry-Local) - オンデバイスモデル実行基盤
- [Tauri](https://tauri.app/) - デスクトップアプリシェル
- [axum](https://github.com/tokio-rs/axum) - HTTP サーバー
- [symphonia](https://github.com/pdeljanov/Symphonia) - 音声デコード
- [rusqlite](https://github.com/rusqlite/rusqlite) - SQLite バインディング
