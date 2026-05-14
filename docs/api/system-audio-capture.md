# システム音声キャプチャ API

## 概要

Windows の既定再生デバイスを WASAPI loopback で録音し、WAV として返す。e-learning、動画、リモート会議など、PC上で再生されている音声を仮想マイクアプリなしで文字起こしするためのAPI。

録音後の文字起こし、ノート保存、後処理は既存の `POST /api/transcribe` を使用する。

## 設計方針

- 対象OSは Windows。
- キャプチャ対象は Windows の既定再生デバイス。
- 内部実装は `cpal` の WASAPI loopback を使用する。
- axum の共有 state には `cpal::Stream` を直接置かず、専用スレッドに閉じ込める。
- API 側は開始、停止、キャンセル、状態確認だけを担当する。
- ライブ字幕では停止を待たず、`drain` で録音済みサンプルだけを取り出す。
- 生成する音声は mono PCM16 WAV。

## GET /api/system-audio/status

### レスポンス

```json
{
  "supported": true,
  "active": false,
  "device_name": null,
  "sample_rate": null,
  "started_at": null,
  "elapsed_ms": null
}
```

## POST /api/system-audio/start

### リクエスト

```json
{
  "max_seconds": 300
}
```

- `max_seconds` は任意。1〜7200 秒に丸める。

### レスポンス

```json
{
  "supported": true,
  "active": true,
  "device_name": "Speakers",
  "sample_rate": 48000,
  "started_at": 1778700000000
}
```

### エラー

- 既に録音中
- 既定の再生デバイスがない
- WASAPI loopback ストリームを作成できない

## POST /api/system-audio/stop

### レスポンス

- **200 OK**
- `Content-Type: audio/wav`
- `Content-Disposition: attachment; filename="system-audio.wav"`
- `X-SeeDraft-Sample-Rate`
- `X-SeeDraft-Sample-Count`
- `X-SeeDraft-Duration-Seconds`

ボディは mono PCM16 WAV。

### エラー

- 録音が開始されていない
- 音声サンプルが取得できていない

## POST /api/system-audio/drain

録音を継続したまま、前回の `drain` 以降に取得された PCM16 mono サンプルを返す。ライブ字幕でリアルタイム処理するために使用する。

### レスポンス

- **200 OK**
- `Content-Type: application/octet-stream`
- `X-SeeDraft-Sample-Rate`
- `X-SeeDraft-Sample-Count`

ボディは WAV ヘッダなしの little-endian PCM16 mono。新しいサンプルがない場合、ボディは空で `X-SeeDraft-Sample-Count: 0`。

## POST /api/system-audio/cancel

録音中のPC音声キャプチャを破棄する。文字起こしには送信しない。

### レスポンス

```json
{
  "cancelled": true
}
```

## 実装場所

- `src/system_audio.rs` — WASAPI loopback キャプチャ管理
- `src/main.rs` — `/api/system-audio/*` ハンドラとルーティング
- `src/ui/index.html` — 入力ソース切替と録音ボタン連携
