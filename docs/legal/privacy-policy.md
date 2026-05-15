# SeeDraft Privacy Policy

Last updated: 2026-05-15

> Publication checklist: before submitting this policy to Microsoft Store, replace
> `TODO: privacy contact` with a real support email, contact form, or other
> private contact channel controlled by the publisher.

## 日本語

### 1. 適用範囲

本プライバシーポリシーは、Windows デスクトップアプリケーション
「SeeDraft」（以下「本アプリ」）に適用されます。本アプリは、音声の
文字起こし、整形、翻訳、抽出、ノート作成、ドラフト作成を端末上で行う
ローカルファーストのアプリケーションです。

Microsoft Store、Windows、Microsoft Foundry Local、GitHub など、本アプリ
以外のサービスや配布基盤には、それぞれの提供者のプライバシーポリシーが
適用されます。

### 2. 基本方針

本アプリは、ユーザーの音声、文字起こし結果、ノート、ドラフト、翻訳結果
などのユーザーコンテンツを、本アプリの提供者が運営するクラウドサービスへ
送信しません。

本アプリは、アカウント登録、広告、行動追跡、アクセス解析、遠隔測定
（テレメトリ）を目的としたデータ収集を行いません。また、ユーザー
コンテンツを販売しません。

### 3. 本アプリが端末内で取り扱う情報

本アプリは、機能提供のため、次の情報をユーザーの端末内で取り扱います。

- マイク入力、PC再生音声、ユーザーが選択またはドラッグアンドドロップした音声ファイル
- 文字起こし結果、整形結果、翻訳結果、抽出結果、ライブ字幕の内容
- プロジェクト、ノート、タグ、ノート間リンク、ドラフト、翻訳履歴、ライブ字幕セッション
- 文字起こしプロンプト、専門用語、追加指示、テーマ、カスタム後処理ステップ
- 使用モデル、言語、マイク、出力先、表示テーマ、ショートカット、起動設定などのアプリ設定
- AIモデルおよび実行環境のキャッシュ、ダウンロード進捗や互換性診断に関するローカル状態
- クリップボードへコピーまたはカーソル位置へ挿入するためにユーザーが明示的に出力したテキスト

### 4. 保存場所と保存期間

本アプリの主なユーザーデータは、Windows のアプリデータ領域に
`seedraft.sqlite` および `settings.json` として保存されます。ロケール
ファイルは実行ファイル付近、またはアプリデータ領域の `locales/` に保存
される場合があります。AIモデルは Foundry Local のモデルキャッシュに保存
されます。

音声ファイルは文字起こし処理のため一時フォルダー
`seedraft-voice-to-text/` に一時保存され、処理後に削除されます。異常終了、
OS、セキュリティソフト、ファイルロックなどの事情により、一時ファイルの
削除が完了しない場合があります。

ノート、ドラフト、翻訳履歴、ライブ字幕セッション、設定、モデルキャッシュ
などは、ユーザーがアプリ内で削除する、関連ファイルを削除する、または
本アプリをアンインストールしてアプリデータを削除するまで端末内に残る場合
があります。

### 5. 外部送信とネットワーク接続

本アプリは、通常の文字起こし、整形、翻訳、抽出、ドラフト作成のために、
ユーザーの音声データ、文字データ、ノート、ドラフトを本アプリ提供者の
サーバーへ送信しません。これらの処理は、端末上のローカルHTTPサーバー
（ループバックアドレス）とローカルAI実行環境を通じて行われます。

ただし、次の場合にはインターネット接続が発生することがあります。

- 初回利用時またはモデル変更時に、Foundry Local 経由でAIモデルをダウンロードする場合
- 実行環境の準備や更新のために、Windows、Microsoft Store、winget、または
  Foundry Local 関連の仕組みを利用する場合
- ユーザーがエクスポートしたファイルやコピーしたテキストを、別のアプリや
  サービスで共有する場合

モデルダウンロードなどの外部接続では、接続先サービスにより、IPアドレス、
デバイス情報、ダウンロード対象、ログなどの通信上必要な情報が処理される
場合があります。これらは各サービス提供者のポリシーに従って処理されます。

### 6. アクセス権限

本アプリは、ユーザーが選択した機能に応じて、次の権限またはOS機能を使用
します。

- マイク: 音声録音と文字起こしのため
- PC再生音声キャプチャ: ユーザーが選択した場合に、Windows の既定再生音声を文字起こしするため
- ファイルおよびフォルダー選択: 音声ファイルの読み込み、出力先の選択、ドラフトのエクスポートのため
- クリップボードおよびキーボード入力補助: ユーザーの明示操作により、文字起こし結果をコピーまたは挿入するため
- グローバルショートカット: 録音、表示、設定表示などの操作をすばやく実行するため

### 7. 第三者提供

本アプリの提供者は、ユーザーコンテンツを第三者に販売または貸与しません。
本アプリは広告ネットワークやアクセス解析サービスを組み込んでいません。

モデルダウンロード、OS機能、配布基盤、ユーザーが選択した外部共有先に
関しては、それぞれの第三者が独自に情報を処理する場合があります。

### 8. セキュリティ

本アプリは、ユーザーデータを主に端末内に保存し、Windows のファイル権限や
ユーザーアカウントの保護に依存します。本アプリは、保存データに対して独自の
エンドツーエンド暗号化やクラウドバックアップを提供しません。機密情報を扱う
場合は、Windows のデバイス暗号化、BitLocker、OSアカウント保護、バックアップ
管理など、利用環境に応じた保護を行ってください。

### 9. ユーザーによる管理

ユーザーは、アプリ内の削除機能、エクスポート機能、設定画面、またはWindows
のファイル管理機能を通じて、端末内に保存されたデータを管理できます。

本アプリの提供者は、通常、ユーザーの端末内に保存された音声、文字起こし、
ノート、ドラフト、翻訳履歴へアクセスできません。そのため、これらのデータの
開示、訂正、削除、移行は、ユーザーの端末上でユーザー自身が行う必要が
あります。

### 10. 子どものプライバシー

本アプリは、子どもを対象として個人情報を意図的に収集するものではありません。
未成年者が本アプリを利用する場合は、保護者または管理者の判断と管理のもとで
利用してください。

### 11. ポリシーの変更

本ポリシーは、機能追加、配布方法の変更、法令またはストア要件の変更に応じて
更新される場合があります。重要な変更がある場合は、配布ページ、リリースノート、
または本ポリシーの更新によりお知らせします。

### 12. お問い合わせ

本ポリシーまたは本アプリのプライバシーに関するお問い合わせは、次の連絡先へ
お願いします。

TODO: privacy contact

## English

### 1. Scope

This Privacy Policy applies to the Windows desktop application "SeeDraft" (the
"App"). SeeDraft is a local-first application for speech transcription, text
refinement, translation, extraction, note taking, and draft composition.

Microsoft Store, Windows, Microsoft Foundry Local, GitHub, and other third-party
services or distribution platforms are governed by their own privacy policies.

### 2. Our Approach

The App does not send your audio, transcripts, notes, drafts, translations, or
other user content to cloud services operated by the App publisher.

The App does not require an account and does not collect data for advertising,
behavioral tracking, analytics, or telemetry. The App publisher does not sell
user content.

### 3. Information Handled Locally

To provide its features, the App handles the following information on your
device:

- Microphone input, PC playback audio, and audio files selected or dropped by you
- Transcripts, refined text, translations, extracted summaries or keywords, and live captions
- Projects, notes, tags, note links, drafts, translation history, and live-caption sessions
- Transcription prompts, terminology, additional instructions, themes, and custom post-processing steps
- App settings such as selected models, language, microphone, output folder, theme, shortcuts, and startup behavior
- AI model files, runtime cache, download progress, and compatibility diagnostics stored locally
- Text that you explicitly copy to the clipboard or insert at the cursor

### 4. Storage and Retention

The App stores main user data in the Windows app data area as `seedraft.sqlite`
and `settings.json`. Locale files may be stored beside the executable or in a
`locales/` directory under the app data area. AI models are stored in the
Foundry Local model cache.

Audio files may be temporarily written to a `seedraft-voice-to-text/` folder
under the system temporary directory for transcription processing and are
deleted after processing. Temporary files may remain if the App exits
unexpectedly or if deletion is blocked by the operating system, security
software, or file locks.

Notes, drafts, translation history, live-caption sessions, settings, and model
caches may remain on your device until you delete them in the App, remove the
related files, or uninstall the App and remove its app data.

### 5. External Transmission and Network Connections

During normal transcription, refinement, translation, extraction, and draft
composition, the App does not transmit your audio, text, notes, or drafts to
servers operated by the App publisher. Processing is performed through a local
HTTP server on the loopback address and local AI runtime components.

Internet connections may occur in the following cases:

- When AI models are downloaded through Foundry Local on first use or after changing models
- When Windows, Microsoft Store, winget, or Foundry Local mechanisms are used to prepare or update runtime components
- When you export files or copy text and then share that content with another app or service

For model downloads or other external connections, the external service may
process network information such as your IP address, device information, target
download, and logs as needed to provide the service. Such processing is governed
by the relevant service provider's policies.

### 6. Permissions

Depending on the features you choose to use, the App may use the following
permissions or operating system features:

- Microphone: to record audio for transcription
- PC playback audio capture: to transcribe the default Windows playback audio when selected
- File and folder selection: to import audio files, select output folders, and export drafts
- Clipboard and keyboard input assistance: to copy or insert transcription results when you explicitly request it
- Global shortcuts: to start recording, show the App, open settings, or perform similar actions quickly

### 7. Sharing With Third Parties

The App publisher does not sell or rent user content to third parties. The App
does not include advertising networks or analytics services.

Third-party processing may occur in connection with model downloads, operating
system features, distribution platforms, or external services that you choose to
use when sharing exported files or copied text.

### 8. Security

The App primarily stores user data on your device and relies on Windows file
permissions and user account protections. The App does not provide its own
end-to-end encryption or cloud backup for stored data. If you handle sensitive
information, consider using Windows device encryption, BitLocker, OS account
protection, and appropriate backup practices.

### 9. Your Controls

You can manage locally stored data through the App's delete and export features,
settings, and Windows file management tools.

The App publisher generally cannot access audio, transcripts, notes, drafts, or
translation history stored on your device. Therefore, requests to access,
correct, delete, or move such data generally need to be completed by you on your
own device.

### 10. Children's Privacy

The App is not designed to intentionally collect personal information from
children. If a minor uses the App, it should be under the supervision and
control of a parent, guardian, or administrator.

### 11. Changes

This policy may be updated when features, distribution methods, laws, or store
requirements change. Material changes may be communicated through the
distribution page, release notes, or an updated version of this policy.

### 12. Contact

For privacy questions about this policy or the App, contact:

TODO: privacy contact
