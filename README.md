# iceberg-poc-public

以下技術記事を参考にDI注入を意識したコードを検討
https://zenn.dev/penginpenguin/articles/77d4a9b1e90e3a#%E3%82%B5%E3%83%B3%E3%83%97%E3%83%AB%E3%82%B3%E3%83%BC%E3%83%89

以下2パターンで検討（DI注入の観点では1のほうがシンプル）
1. S3からtmpにダウンロードしてロードするパターン
1. S3から直接ロードするパターン
