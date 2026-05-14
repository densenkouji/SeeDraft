# メイン画面

## 概要

アプリ起動時に表示されるメイン画面。axum が配信する HTML を Tauri ウィンドウで表示する。

## URL / ルート

- `GET /index` — メイン画面の HTML を返す

## レイアウト

- ノート一覧、本文エディタ、グラフ/ドラフト/ライブ字幕などのワークスペースを表示
- 文字起こし本文の下に、コピー、クリア、ノート作成、クイック後処理、入力ソース、録音ボタンを配置
- メイン録音とライブ字幕の入力ソースは `マイク` / `PC音声` を切り替え可能
- レスポンシブ対応

## 操作仕様

| 操作 | 結果 |
|------|------|
| アプリ起動 | axum サーバー起動後、自動的にこの画面を表示 |
| 入力ソースで `マイク` を選択して録音 | WebView の MediaRecorder でマイク音声を録音し、既存の `/api/transcribe` に送信 |
| 入力ソースで `PC音声` を選択して録音 | `/api/system-audio/start` で Windows の再生音声を録音し、停止時に `/api/system-audio/stop` のWAVを `/api/transcribe` に送信 |
| ライブ字幕で `マイク` を選択して開始 | WebAudio でマイク音声をバッファし、発話単位で `/api/live/chunk` に送信 |
| ライブ字幕で `PC音声` を選択して開始 | `/api/system-audio/drain` でPC音声を継続取得し、発話単位で `/api/live/chunk` に送信 |
| ウィンドウ閉じる | axum サーバーを graceful shutdown して終了 |

## API連携

- `POST /api/transcribe` — 音声ファイル、マイク録音、PC音声録音を文字起こし
- `GET /api/system-audio/status` — PC音声キャプチャの対応状況を確認
- `POST /api/system-audio/start` — PC音声キャプチャ開始
- `POST /api/system-audio/drain` — 録音中のPC音声PCMチャンク取得
- `POST /api/system-audio/stop` — PC音声キャプチャ停止、WAV取得
- `POST /api/system-audio/cancel` — エラー時や中断時にPC音声キャプチャを破棄

## エラー処理

- サーバー起動タイムアウト (10秒) 時はアプリ終了
- ポート競合時はエラーメッセージを出力して終了
- PC音声が取得できない場合はエラーバナーと録音情報欄に表示
