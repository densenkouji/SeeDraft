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
