# Foundry Local v1.1.0 対応設計

## ステータス
**Draft** - 2026-05-20

## 概要
Foundry Local v1.1.0 で追加されたモデル種別とライブ音声文字起こし API に合わせて、SeeDraft のモデル管理とライブ字幕を更新する。

対応は 2 段階で進める。

1. モデル一覧を `speech` / `live_speech` / `chat` / `embedding` / `vision` / `other` に分類し、後処理 LLM 候補に Embedding や Vision モデルが混ざらないようにする。
2. ライブ字幕を現在の WAV チャンク送信方式から、Foundry Local の Live Transcription Session へ raw PCM16 を継続投入する方式へ移行する。

## 背景
現在の SeeDraft は Foundry Local Rust SDK を path dependency として直接利用し、通常文字起こしは `AudioClient::transcribe()`、後処理は `ChatClient::complete_chat()` で実行している。

既存のライブ字幕は WebView 側で 16kHz mono PCM を作り、無音検出で発話単位の WAV を組み立てて `/api/live/chunk` に送信する。サーバー側は各チャンクを一時ファイル化し、通常文字起こしと同じ `transcribe()` に渡す。

Foundry Local v1.1.0 では次の影響がある。

- Embedding モデルが追加されるため、音声以外をすべて LLM とみなす分類は危険。
- Vision Language モデルが追加されるため、テキスト後処理に使えるモデルかを明示判定する必要がある。
- Live Transcription API が追加され、raw PCM chunks をセッションへ append し、partial/final 結果を stream として受け取れる。
- WebGPU EP がプラグイン化されたため、互換性判定と EP 登録状態の表示は維持する。

## ゴール
- モデル一覧 UI で用途別にモデルを正しく表示する。
- 後処理、翻訳、抽出、補完、ドラフト生成では chat-capable なモデルだけを候補にする。
- Embedding モデルは将来のセマンティック検索/RAG 用に表示できるが、後処理モデル候補には含めない。
- Vision モデルは将来拡張用に表示できるが、現時点では SeeDraft の主要処理候補には含めない。
- ライブ字幕の待ち時間、重複、発話境界の不自然さを減らす。
- 既存の保存済みライブ字幕セッション、翻訳、PC音声キャプチャとの連携を維持する。

## 非ゴール
- Responses API への全面移行は行わない。既存の後処理は `complete_chat()` を維持する。
- Embedding を使ったノート検索や RAG は今回の主対象にしない。
- Vision 入力 UI は今回作らない。
- 通常録音の `/api/transcribe` は現行方式を維持する。

## モデル分類設計

### 分類
`ModelInfo.category` を次の値に拡張する。

| category | 用途 | UI 表示 | 後処理候補 |
|---|---|---|---|
| `speech` | ファイル/短尺音声の文字起こし | 音声認識 | いいえ |
| `live_speech` | ライブ字幕用ストリーミング ASR | ライブ音声認識 | いいえ |
| `chat` | 整形、翻訳、抽出、補完、ドラフト生成 | テキスト処理 | はい |
| `embedding` | ベクトル化、セマンティック検索 | Embedding | いいえ |
| `vision` | 画像理解、将来の multimodal 入力 | Vision | いいえ |
| `other` | 未分類、未対応 | その他 | いいえ |

### 判定ルール
`input_modalities`、`output_modalities`、`capabilities`、`task`、alias を組み合わせて conservative に判定する。

優先順位:

1. alias または capabilities/task に `embedding` を含む場合は `embedding`
2. input modalities に `image`、または alias/capabilities/task に `vision` / `vl` を含む場合は `vision`
3. alias に `nemotron-speech-streaming`、または task/capabilities に `live` と `transcription` / `asr` を含む場合は `live_speech`
4. alias が `whisper` で始まる、または input modalities に `audio` を含む場合は `speech`
5. input/output modalities が text 中心で、embedding/vision/live_speech ではない場合は `chat`
6. それ以外は `other`

注意: live ASR モデルは audio input/text output なので、通常 `speech` より前に `live_speech` を判定する。

### API 変更
`GET /api/models` の `ModelInfo` に後方互換の `category` は残し、追加メタデータを加える。

```json
{
  "alias": "qwen3-0.6b-embedding",
  "category": "embedding",
  "input_modalities": "text",
  "output_modalities": "embedding",
  "capabilities": "embedding",
  "downloaded": false,
  "loaded": false,
  "active": false,
  "compatible": true
}
```

追加フィールド:

- `input_modalities: string | null`
- `output_modalities: string | null`
- `capabilities: string | null`
- `task: string | null`
- `selectable_for: string[]`

`selectable_for` は UI 判定を単純化するための派生値にする。

| 値 | 条件 |
|---|---|
| `transcription` | `speech` |
| `live_transcription` | `live_speech` |
| `postprocess` | `chat` |
| `embedding` | `embedding` |
| `vision` | `vision` |

### Backend 変更
- `model_is_speech()` を `classify_model()` に置き換える。
- `downloaded_compatible_model_aliases(speech: bool)` を用途指定型へ変更する。

```rust
enum ModelUse {
    Transcription,
    LiveTranscription,
    Postprocess,
    Embedding,
    Vision,
}
```

- `resolve_model_alias()` は `ModelUse` を受け取る。
- `ensure_chat_model_by_alias()` は category `chat` 以外を explicit request された場合に bad request とする。
- `ensure_speech_model()` は category `speech` のみを対象にする。
- 新規 `ensure_live_speech_model()` は category `live_speech` を対象にする。
- `startup_model_requirements()` は当面 `speech` と `chat` だけを必須にする。`live_speech` はライブ字幕開始時に必要なら促す。

### Frontend 変更
- モデル設定画面を用途別セクションに分ける。
  - 音声認識
  - ライブ音声認識
  - テキスト処理
  - Embedding
  - Vision / その他
- 後処理モデル選択 (`refine` / `translate` / `complete` / `custom`) は `selectable_for` に `postprocess` を含むものだけを表示する。
- ライブ字幕の音声モデル選択は `live_transcription` があればそれを優先し、なければ既存の `speech` モデルにフォールバックする。
- Embedding/Vision モデルのカードはダウンロード・削除・互換性表示だけにし、「使用」ボタンは出さない。

## ライブ字幕改善設計

### 現行方式
現在の流れ:

1. WebView が AudioContext / PC音声 drain で PCM を取得
2. ブラウザ側で音量を見て発話単位に切り出す
3. 発話を WAV Blob にして `/api/live/chunk` へ送信
4. サーバーが一時ファイルへ書き出し、`AudioClient::transcribe()` を実行
5. 返ってきた確定テキストをセグメントとして UI に追加

課題:

- 発話が終わるまで結果が出ない。
- チャンクごとに一時ファイルと WAV ヘッダが発生する。
- 無音検出の境界次第で欠落、重複、短すぎる音声の空振りが起きる。
- Foundry Local 側の partial/final 結果を使えていない。

### 新方式
ライブ字幕開始時に Foundry Local の live transcription session を作り、raw PCM16 mono 16kHz を継続投入する。

Backend:

```text
POST /api/live/start
  -> LiveSessionState 作成
  -> ensure_live_speech_model()
  -> model.create_audio_client().create_live_transcription_session()
  -> session.settings = { sample_rate: 16000, channels: 1, bits_per_sample: 16, language }
  -> session.start(None)
  -> session.get_stream() を background task で読む
  -> 結果は tokio broadcast または mpsc queue に投入

POST /api/live/audio
  -> body: application/octet-stream
  -> raw little-endian PCM16 mono 16kHz
  -> session.append(bytes, None)

GET /api/live/events?session_id=...
  -> SSE
  -> partial/final/error を UI に配信

POST /api/live/stop
  -> session.stop(None)
  -> final 結果を flush
  -> SQLite に live_session/live_segments を保存
```

Frontend:

1. `startLiveSession()` で `/api/live/start` を呼ぶ。
2. `EventSource("/api/live/events?...")` を開く。
3. マイク/PC音声から得た Float32 PCM を 16kHz mono PCM16 bytes に変換する。
4. 100ms から 250ms 程度の小さい塊で `/api/live/audio` に送る。
5. `partial` は現在行として上書き、`final` はセグメントとして確定表示する。
6. 同時翻訳は final segment を対象に既存 `/api/live/translate` を実行する。

### API 詳細

#### POST /api/live/start
既存レスポンスに `mode` と `streaming` を追加する。

```json
{
  "session_id": "uuid",
  "started_at": 1760000000000,
  "mode": "native_live",
  "streaming": true
}
```

`live_speech` モデルが利用できない場合は、次のいずれかを選ぶ。

- Phase 1: `mode: "chunked"` として現行 `/api/live/chunk` へ自動フォールバック
- Phase 2: UI で streaming model のダウンロードを促す

初回実装では安全性のため Phase 1 を採用する。

#### POST /api/live/audio
`Content-Type: application/octet-stream`

Headers:

- `X-SeeDraft-Live-Session: <session_id>`
- `X-SeeDraft-Sample-Rate: 16000`
- `X-SeeDraft-Channels: 1`
- `X-SeeDraft-Bits-Per-Sample: 16`

レスポンス:

```json
{ "ok": true, "queued_bytes": 3200 }
```

#### GET /api/live/events
SSE event types:

```json
{
  "type": "partial",
  "sequence": 12,
  "text": "途中結果",
  "start_ms": 1200,
  "end_ms": 2400,
  "is_final": false
}
```

```json
{
  "type": "final",
  "sequence": 13,
  "text": "確定した文字起こしです。",
  "start_ms": 1200,
  "end_ms": 2600,
  "is_final": true
}
```

```json
{
  "type": "error",
  "message": "Live transcription stream failed",
  "transient": true
}
```

### LiveSessionState
`LiveSessionState` に native live 用の状態を追加する。

```rust
enum LiveBackendMode {
    Chunked,
    NativeLive,
}

struct LiveNativeState {
    session: Arc<foundry_local_sdk::LiveAudioTranscriptionSession>,
    reader: tokio::task::JoinHandle<()>,
    tx: tokio::sync::broadcast::Sender<LiveEvent>,
}
```

`LiveSessionState` は `native: Option<LiveNativeState>` を持つ。既存のセグメント保存、翻訳キュー、停止処理は共通化する。

### セグメント確定
- `partial` は UI 表示専用。SQLite には保存しない。
- `final` は `LiveSegment` として `session.segments` に追加する。
- Foundry Local の `start_time` / `end_time` がある場合はそれを優先して ms に変換する。
- 時刻がない場合は受信時刻と `session.started_at` から推定する。
- 空文字、前回 final と同一の短文、前後空白だけの結果は破棄する。

### 翻訳
- 翻訳は final segment 単位で行う。
- 既存の `/api/live/translate` と `translate_live_text()` は維持する。
- partial 翻訳は行わない。低遅延化より安定性を優先する。
- 将来、final が長すぎる場合は文単位分割して翻訳する。

### フォールバック
新 API は以下の場合に現行 chunked mode へ戻す。

- `live_speech` モデルがカタログにない。
- live model の download/load/start に失敗した。
- `LiveAudioTranscriptionSession` stream が開始直後に失敗した。

フォールバック時も UI API は同じ `/api/live/start` を使う。レスポンスの `mode` が `chunked` の場合だけ、フロントは現行の発話単位 `/api/live/chunk` 送信を続ける。

## 実装順

### Phase 1: モデル分類
1. `ModelCategory` / `ModelUse` を Rust 側に追加する。
2. `classify_model()` と `selectable_for()` を追加する。
3. `ModelInfo` に modality/capability/task/selectable_for を追加する。
4. `downloaded_compatible_model_aliases()` と `resolve_model_alias()` を用途指定へ変更する。
5. UI のモデル一覧を category ベースではなく `selectable_for` ベースでフィルタする。
6. Embedding/Vision/Other の表示セクションを追加する。

### Phase 2: Live Transcription backend
1. `LiveBackendMode` と `LiveNativeState` を追加する。
2. `ensure_live_speech_model()` を追加する。
3. `/api/live/start` で native live session を開始し、失敗時は chunked にフォールバックする。
4. `/api/live/audio` と `/api/live/events` を追加する。
5. live stream reader から `partial` / `final` / `error` イベントを発行する。
6. `/api/live/stop` で native session を確実に stop し、reader task を終了する。

### Phase 3: Live Transcription frontend
1. start response の `mode` を見て native/chunked を切り替える。
2. native mode では PCM16 bytes を小さく送る送信ループを追加する。
3. SSE events で partial 行と final segment を描画する。
4. final segment で既存翻訳処理を呼ぶ。
5. stop 時に audio sender、EventSource、capture node を順に閉じる。

### Phase 4: 検証
1. `cargo fmt`
2. `cargo check`
3. `/api/models` で Embedding/Vision が後処理候補に混ざらないことを確認
4. ライブ字幕 mic native mode の開始/停止確認
5. ライブ字幕 PC音声 native mode の開始/停止確認
6. live_speech がない環境で chunked fallback 確認
7. 保存済みライブセッションの表示、リネーム、削除、プロジェクト化確認

## リスクと対策

| リスク | 対策 |
|---|---|
| catalog metadata の表記ゆれ | alias/task/capabilities/modalities を複合判定し、未知は `other` に倒す |
| live_speech model が重い、または未ダウンロード | 初回は chunked fallback を残す |
| partial が頻繁に来て UI がちらつく | partial は 100ms 程度で throttle し、final で確定する |
| native session stop 漏れ | `/api/live/stop` と Drop 相当の cleanup path で `stop(None)` を必ず呼ぶ |
| PC音声 drain と native append の速度差 | backend append が詰まったらフロント送信キューを捨てずに待つ。一定以上詰まった場合のみ警告 |
| Embedding/Vision を誤って LLM として load | `ensure_chat_model_by_alias()` で `selectable_for` 検証を行う |

## 受け入れ条件
- `/api/models` の各モデルに `category` と `selectable_for` が返る。
- `qwen3-0.6b-embedding` 系が `embedding` として表示され、後処理モデル候補に出ない。
- `qwen3.5-vision` 系が `vision` として表示され、後処理モデル候補に出ない。
- `nemotron-speech-streaming` 系が `live_speech` として表示され、ライブ字幕モデル候補に出る。
- 既存 Whisper モデルは通常文字起こし候補として残る。
- ライブ字幕 native mode で partial/final が表示される。
- native mode が使えない環境でも chunked mode でライブ字幕を継続できる。
- アプリ終了時に live session と loaded model が unload される。
