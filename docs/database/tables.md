# データベース設計書

## 概要

現時点では SeeDraft はデータベースを使用していない。
機能拡張時にここにテーブル定義を追記する。

## テーブル一覧

(未定義)

## テーブル定義の書き方

```markdown
### テーブル名: example_table

| カラム名 | 型 | NULL | デフォルト | 説明 |
|---------|-----|------|-----------|------|
| id | INTEGER | NO | AUTO INCREMENT | 主キー |
| name | TEXT | NO | - | 名前 |
| created_at | DATETIME | NO | CURRENT_TIMESTAMP | 作成日時 |
```
