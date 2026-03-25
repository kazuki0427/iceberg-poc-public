# tsv_to_iceberg_load

S3上のTSVファイルをIceberg（Glue Data Catalog）テーブルにロードするLambda関数。

仕様詳細: [docs/specs/tsv_to_iceberg_load.md](../../docs/specs/tsv_to_iceberg_load.md)

---

## 処理概要

```
S3 (TSV)
  └─► [boto3] ダウンロード → /tmp
        └─► [DuckDB] read_csv → PyArrow Table
              └─► [PyIceberg] overwrite → Iceberg (Glue Data Catalog)
```

既存データを全件削除してから追記する洗い替え方式のため、同一ファイルを再実行しても重複が発生しない。

---

## ディレクトリ構成

```
tsv_to_iceberg_load/
├── src/
│   ├── handler.py          # Lambdaエントリーポイント
│   ├── loader.py           # ビジネスロジック
│   └── clients.py          # AWSクライアント生成（DI用）
├── tests/
│   ├── conftest.py         # pytestフィクスチャ
│   └── test_loader.py      # テストコード
├── pytest.ini
├── requirements.txt        # 本番依存ライブラリ
└── requirements-dev.txt    # 開発・テスト用ライブラリ
```

---

## 環境変数

| 変数名 | 必須 | 説明 |
|--------|------|------|
| `GLUE_REGION` | ✓ | Glue CatalogのAWSリージョン（例: `ap-northeast-1`） |
| `GLUE_DATABASE` | ✓ | Glue Data Catalogのデータベース名 |
| `GLUE_TABLE` | ✓ | ロード先Icebergテーブル名 |

---

## イベント形式

Step Functionsまたは手動実行時のインプット例:

```json
{
  "s3_bucket": "my-input-bucket",
  "s3_key": "path/to/input.tsv"
}
```

---

## ローカル開発・テスト

### セットアップ

```bash
cd lambda/tsv_to_iceberg_load
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### テスト実行

```bash
pytest
```

### テストの構成

| テスト対象 | ケース |
|-----------|--------|
| `read_tsv_with_duckdb` | 正常読み込み、空ファイル（0件） |
| `download_from_s3` | 正常ダウンロード、S3キー未存在 |
| `load_to_iceberg` | データ追加、洗い替え（重複なし）、テーブル未存在 |
| `load_tsv_to_iceberg` | 正常系、冪等性、0件スキップ、S3未存在、tmpファイル削除 |

- S3のモック: [moto](https://github.com/getmoto/moto)
- Catalogのモック: PyIceberg `SqlCatalog`（SQLiteバックエンド）

---

## Lambdaレイヤー構築

```bash
mkdir python
pip install -t python \
  --platform manylinux2014_x86_64 \
  --only-binary=:all: \
  "pyiceberg[glue,duckdb]"

# LambdaランタイムにはBoto3が含まれるため削除（250MB制限対策）
rm -rf python/boto3 python/botocore
zip -r layer.zip python/
```

---

## IAM権限

| AWSサービス | 必要な権限 |
|------------|-----------|
| S3（入力バケット） | `s3:GetObject`, `s3:ListBucket` |
| S3（Icebergデータバケット） | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` |
| Glue | `glue:GetTable`, `glue:GetDatabase`, `glue:UpdateTable` |
