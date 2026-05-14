# API 仕様書 Index

SeeDraft の API エンドポイント一覧。

| メソッド | パス | 概要 | ファイル |
|---------|------|------|----------|
| GET | `/index` | メイン画面 HTML を返す | [endpoints.md](endpoints.md) |
| GET | `/api/system-audio/status` | PC音声キャプチャの対応状況と実行状態を返す | [system-audio-capture.md](system-audio-capture.md) |
| POST | `/api/system-audio/start` | Windows WASAPI loopback によるPC音声キャプチャを開始する | [system-audio-capture.md](system-audio-capture.md) |
| POST | `/api/system-audio/drain` | 録音中のPC音声をPCMチャンクとして取得する | [system-audio-capture.md](system-audio-capture.md) |
| POST | `/api/system-audio/stop` | PC音声キャプチャを停止し、WAVを返す | [system-audio-capture.md](system-audio-capture.md) |
| POST | `/api/system-audio/cancel` | PC音声キャプチャを破棄して停止する | [system-audio-capture.md](system-audio-capture.md) |

## API 仕様書の書き方

各 API 仕様書には以下を記載する:

1. **エンドポイント** — メソッド、パス
2. **リクエスト** — パラメータ、ヘッダ、ボディ
3. **レスポンス** — ステータスコード、ボディ
4. **エラー** — エラーレスポンスの形式
5. **実装場所** — 対応する Rust のハンドラ関数
