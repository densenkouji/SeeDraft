# 後処理パイプライン API 仕様書

## ステータス
**Approved** - 2026-05-10

## 概要
文字起こし結果に対して、整形・翻訳などの後処理を柔軟に組み合わせて実行できるパイプラインAPIです。

## エンドポイント

### POST /api/process

#### 説明
複数の処理ステップをパイプラインとして順次実行します。

#### リクエスト

```json
{
  "text": "処理対象のテキスト",
  "pipeline": [
    {
      "type": "refine",
      "text": "処理対象のテキスト（省略可、textフィールドを使用）",
      "remove_fillers": true,
      "voice_commands": true,
      "style": "natural",
      "custom_terms": "SeeDraft, Foundry Local",
      "custom_instruction": "箇条書きではなく本文でまとめる"
    },
    {
      "type": "translate",
      "text": "処理対象のテキスト（省略可、前ステップの結果を使用）",
      "source_language": "auto",
      "target_language": "en",
      "custom_terms": "SeeDraft, Foundry Local",
      "custom_instruction": "製品名は英語表記のまま"
    }
  ]
}
```

#### レスポンス

```json
{
  "results": [
    {
      "step_type": "refine",
      "text": "整形後のテキスト"
    },
    {
      "step_type": "translate",
      "text": "翻訳後のテキスト"
    }
  ]
}
```

#### 処理ステップの種類

##### refine（整形）
文字起こし結果を読みやすく整形します。

**パラメータ:**
- `text` (string, required): 処理対象のテキスト
- `remove_fillers` (boolean, optional, default: true): フィラー除去の有無
- `voice_commands` (boolean, optional, default: true): 音声コマンド反映の有無
- `style` (string, optional, default: "natural"): スタイル
  - `natural` - 読みやすい自然文
  - `polite` - 丁寧な表現
  - `business` - ビジネス文書
  - `minutes` - 議事録向け
- `custom_terms` (string, optional): 保持する専門用語・固有名詞（カンマ区切り）
- `custom_instruction` (string, optional): 追加の整形指示

**処理内容:**
1. フィラー除去（有効な場合）
2. 音声コマンド適用（有効な場合）
3. LLMによる文章整形
4. カスタム指示の適用

##### translate（翻訳）
テキストを指定言語に翻訳します。

**パラメータ:**
- `text` (string, required): 処理対象のテキスト
- `source_language` (string, optional, default: "auto"): 翻訳元言語
  - `auto` - 自動判定
  - `ja` - 日本語
  - `en` - English
  - `ko` - 한국어
  - `zh` - 中文
- `target_language` (string, optional, default: "ja"): 翻訳先言語
  - `ja` - 日本語
  - `en` - English
  - `ko` - 한국어
  - `zh` - 中文
- `custom_terms` (string, optional): 保持する固有名詞・専門用語
- `custom_instruction` (string, optional): 追加の翻訳指示

**処理内容:**
1. 言語判定（source_language が auto の場合）
2. LLMによる翻訳
3. 字幕向けの自然な表現に調整
4. カスタム指示の適用

#### エラーレスポンス

```json
{
  "error": "エラーメッセージ"
}
```

**エラーコード:**
- `400 Bad Request` - リクエストパラメータが不正
- `500 Internal Server Error` - サーバー内部エラー

## 使用例

### 例1: 整形のみ

```javascript
const response = await fetch("/api/process", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    text: "えー、今日はですね、あー、プロジェクトの進捗を報告します",
    pipeline: [
      {
        type: "refine",
        text: "えー、今日はですね、あー、プロジェクトの進捗を報告します",
        remove_fillers: true,
        voice_commands: false,
        style: "business"
      }
    ]
  })
});

const result = await response.json();
// result.results[0].text: "今日は、プロジェクトの進捗を報告します。"
```

### 例2: 翻訳のみ

```javascript
const response = await fetch("/api/process", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    text: "今日は良い天気ですね",
    pipeline: [
      {
        type: "translate",
        text: "今日は良い天気ですね",
        source_language: "ja",
        target_language: "en"
      }
    ]
  })
});

const result = await response.json();
// result.results[0].text: "It's a nice day today"
```

### 例3: 整形 → 翻訳

```javascript
const response = await fetch("/api/process", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    text: "えー、SeeDraftはですね、あー、文字起こしツールです",
    pipeline: [
      {
        type: "refine",
        text: "えー、SeeDraftはですね、あー、文字起こしツールです",
        remove_fillers: true,
        custom_terms: "SeeDraft",
        style: "business"
      },
      {
        type: "translate",
        source_language: "ja",
        target_language: "en",
        custom_terms: "SeeDraft"
      }
    ]
  })
});

const result = await response.json();
// result.results[0].text: "SeeDraftは文字起こしツールです。"
// result.results[1].text: "SeeDraft is a transcription tool."
```

## パイプライン処理の仕組み

### 処理フロー

```
入力テキスト
    ↓
┌─────────────────┐
│ Step 1: Refine  │
│  - フィラー除去  │
│  - LLM整形      │
└────────┬────────┘
         ↓ 整形後のテキスト
┌─────────────────┐
│ Step 2: Trans.  │
│  - LLM翻訳      │
└────────┬────────┘
         ↓ 翻訳後のテキスト
    結果配列
```

### 重要な仕様

1. **順次処理**: パイプライン内のステップは順番に実行されます
2. **テキスト引き継ぎ**: 各ステップは前のステップの出力を入力として使用します
3. **エラー処理**: いずれかのステップでエラーが発生した場合、パイプライン全体が中断されます
4. **結果の保持**: 各ステップの結果はすべて配列で返されます

## パフォーマンス

### 処理時間の目安

- **整形**: 1-3秒（テキスト長により変動）
- **翻訳**: 1-3秒（テキスト長により変動）
- **整形 → 翻訳**: 2-6秒（合計）

### 制約事項

- **最大テキスト長**: 10,000文字
- **最大パイプライン長**: 10ステップ
- **タイムアウト**: 60秒

## セキュリティ

### 入力検証

- テキストの長さチェック
- パイプラインの深さチェック
- 不正なステップタイプの拒否

### データ保護

- 処理後のテキストはメモリ上で即座に破棄
- ローカル処理のため外部送信なし

## 互換性

### 既存APIとの関係

このパイプラインAPIは既存の個別APIと並行して使用できます：

- `POST /api/refine` - 整形専用（レガシー）
- `POST /api/translate` - 翻訳専用（レガシー）
- `POST /api/process` - パイプライン処理（推奨）

新規実装では `/api/process` の使用を推奨しますが、既存コードとの互換性のため `/api/refine` と `/api/translate` も維持されます。

## 今後の拡張

将来的に追加予定の処理ステップ：

- `summarize` - 要約
- `anonymize` - センシティブ情報除去
- `extract_keywords` - キーワード抽出
- `sentiment_analysis` - 感情分析
- `format_convert` - フォーマット変換（Markdown、JSON等）
