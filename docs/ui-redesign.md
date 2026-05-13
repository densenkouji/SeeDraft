# UI再設計 - シンプルメイン画面 + 設定モーダル

## 実装日
2026-05-10

## 概要
メイン画面をシンプルにし、文字起こし結果の表示と録音機能に特化。各種設定は別画面（モーダル）に分離しました。

## 主な変更点

### 1. メイン画面の構成

#### 表示要素
- **文字起こし結果ボックス** - リアルタイムで結果を大きく表示
- **録音ボタン** - Start/Stop の明確な操作
- **ステータスバッジ** - 現在の状態を表示
- **設定ボタン** - モーダルを開く

#### 操作方法
1. **録音** - 録音ボタンをクリックして Start/Stop
2. **ファイルD&D** - 画面全体にWAV/MP3等をドロップして即座に文字起こし
3. **コピー/クリア** - 結果ボックス内のボタンで操作

### 2. 設定モーダル

#### タブ構成
1. **文字起こし** - Whisperモデル、言語設定、カスタムプロンプト
2. **後処理** - 整形・翻訳の設定、プリセット管理
3. **履歴** - 文字起こし履歴と翻訳履歴の一覧
4. **出力** - 保存先フォルダ、ファイル名設定

#### 開閉方法
- 右上の「⚙ 設定」ボタンで開く
- ヘッダーの「✕」ボタンで閉じる
- モーダル外をクリックで閉じる

### 3. ドラッグ&ドロップ機能

#### 動作
1. 音声ファイルを画面にドラッグ → オーバーレイ表示
2. ドロップ → 自動的に文字起こし開始
3. 録音完了後も自動的に文字起こし開始

#### 対応フォーマット
- mp3
- wav
- m4a
- webm
- その他のaudio/*

### 4. ダークテーマデザイン

#### カラーパレット
- **背景**: `#0f1419` (濃いグレー)
- **コンテナ**: `#0d1117` (ダークグレー)
- **ボーダー**: `#30363d` (ミディアムグレー)
- **テキスト**: `#e6edf3` (明るいグレー)
- **アクセント**: `#238636` (グリーン)

#### デザイン特徴
- 大きな文字サイズ（1.5rem）でリアルタイム結果を見やすく
- 丸みのあるボーダー（border-radius: 12px）
- シンプルなボタンとアイコン
- モダンなダークテーマ

### 5. レスポンシブデザイン

#### モバイル対応（768px以下）
- トップバーを縦並びに
- 録音ボタンを全幅に
- 設定モーダルを画面いっぱいに

## コード構造

### HTML構造
```html
<main class="app-shell">
    <section class="main-workspace">
        <!-- トップバー -->
        <div class="topbar">
            <div class="app-title">...</div>
            <div class="topbar-actions">
                <div class="status-badge">...</div>
                <button class="settings-button">⚙ 設定</button>
            </div>
        </div>

        <!-- ドロップオーバーレイ -->
        <div class="drop-overlay" hidden>...</div>

        <!-- メイン表示エリア -->
        <div class="caption-display">
            <div class="caption-header">
                <span class="caption-label">文字起こし結果</span>
                <div class="caption-actions">
                    <button>コピー</button>
                    <button>クリア</button>
                </div>
            </div>
            <div class="caption-content">...</div>
        </div>

        <!-- コントロールバー -->
        <div class="control-bar">
            <button class="record-button">
                <span class="record-icon">⏺</span>
                <span class="record-label">録音開始</span>
            </button>
            <div class="record-info">準備完了</div>
        </div>
    </section>

    <!-- 設定モーダル -->
    <div class="settings-modal" hidden>
        <div class="settings-container">
            <div class="settings-header">...</div>
            <div class="settings-tabs">...</div>
            <div class="settings-content">...</div>
        </div>
    </div>
</main>
```

### JavaScript主要機能

#### ドラッグ&ドロップ
```javascript
document.addEventListener("dragover", (event) => {
    event.preventDefault();
    dropOverlay.style.display = "flex";
});

document.addEventListener("drop", async (event) => {
    event.preventDefault();
    dropOverlay.style.display = "none";
    const file = event.dataTransfer.files?.[0];
    if (file && file.type.startsWith("audio/")) {
        await transcribeAudio();
    }
});
```

#### 録音後の自動文字起こし
```javascript
mediaRecorder.addEventListener("stop", async () => {
    // 録音完了後、自動的に文字起こし開始
    await transcribeAudio();
});
```

#### 設定モーダルの開閉
```javascript
const openSettings = () => {
    settingsModal.hidden = false;
    document.body.style.overflow = "hidden";
};

const closeSettings = () => {
    settingsModal.hidden = true;
    document.body.style.overflow = "";
};
```

## UI/UX の改善点

### Before（改善前）
- 5タブ構成で情報が分散
- 小さな結果表示エリア
- 設定と結果が混在
- ファイル選択がわかりにくい

### After（改善後）
- ✅ **シンプルなメイン画面** - 文字起こし結果に集中
- ✅ **大きな表示エリア** - 読みやすい1.5remのフォント
- ✅ **直感的な操作** - ドロップするだけで即開始
- ✅ **設定の分離** - モーダルで整理された設定画面
- ✅ **ダークテーマ** - 目に優しいデザイン

## 使用シナリオ

### シナリオ1: 会議の文字起こし
1. アプリを起動（シンプルな画面が表示）
2. 「録音開始」ボタンをクリック
3. 会議が終わったら「録音停止」をクリック
4. 自動的に文字起こしが開始され、結果が大きく表示される
5. 必要に応じて「コピー」ボタンで結果をコピー

### シナリオ2: 既存ファイルの文字起こし
1. アプリを起動
2. 音声ファイルを画面にドラッグ&ドロップ
3. 即座に文字起こしが開始される
4. 結果が画面に表示される

### シナリオ3: 設定のカスタマイズ
1. 右上の「⚙ 設定」ボタンをクリック
2. 「文字起こし」タブでWhisperモデルを変更
3. 「後処理」タブで自動整形をON
4. 「プリセット」で設定を保存
5. モーダルを閉じる
6. 次回から保存した設定が適用される

## パフォーマンス

### メリット
- メイン画面がシンプルなのでレンダリングが高速
- モーダルは必要な時だけ表示
- ドロップ時の即座の反応

### 最適化
- 設定モーダルは初回表示時のみDOM構築
- 履歴リストは最大300pxで自動スクロール
- CSSトランジションは0.15sで軽快

## 今後の拡張案

### 機能追加
1. **ライブ文字起こし** - 録音中にリアルタイムで結果を表示
2. **音声波形表示** - 録音/再生時の視覚的フィードバック
3. **ショートカットキー** - Ctrl+R で録音開始など
4. **テーマ切替** - ライトテーマとダークテーマの選択

### UI改善
1. **結果のハイライト** - キーワードや重要部分を強調
2. **タイムスタンプ表示** - 各発言の時刻を表示
3. **編集機能** - 結果を直接編集可能に
4. **エクスポート形式** - PDF、DOCX、SRTなど

## まとめ

この再設計により、以下が実現されました：

1. ✅ **メイン画面のシンプル化** - 文字起こし結果と録音ボタンのみ
2. ✅ **ドラッグ&ドロップ対応** - 画面全体にファイルをドロップして即座に処理
3. ✅ **設定の分離** - モーダルで整理された設定画面
4. ✅ **ダークテーマ** - 目に優しく、モダンなデザイン
5. ✅ **レスポンシブ** - モバイルでも使いやすい

ユーザーは最小限の操作で文字起こしを開始でき、結果を大きく見やすく表示できるようになりました。
