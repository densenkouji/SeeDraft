# API Endpoints

## GET /index

メイン画面の HTML を返す。

### リクエスト

パラメータなし。

### レスポンス

- **200 OK** — `Content-Type: text/html`

```html
<!doctype html>
<html lang="ja">
  <head><meta charset="utf-8" /><title>...</title></head>
  <body>...</body>
</html>
```

### 実装場所

- `src/main.rs` — `index_handler()`

## PC音声キャプチャAPI

Windows の既定再生デバイスを WASAPI loopback で録音する。詳細は [system-audio-capture.md](system-audio-capture.md) を参照。

| メソッド | パス | 概要 |
|---------|------|------|
| GET | `/api/system-audio/status` | 対応状況と実行状態 |
| POST | `/api/system-audio/start` | キャプチャ開始 |
| POST | `/api/system-audio/drain` | 録音中のPCMチャンク取得 |
| POST | `/api/system-audio/stop` | キャプチャ停止、WAV返却 |
| POST | `/api/system-audio/cancel` | キャプチャ破棄 |
