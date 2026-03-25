import logging
from dataclasses import dataclass, field

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import AlwaysTrue

logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    region: str
    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    # テスト時にmotoサーバーのエンドポイントを指定するためのフィールド
    # 本番環境では None のまま使用する
    endpoint: str | None = None
    use_ssl: bool = True
    url_style: str = "vhost"


def _create_duckdb_connection(s3_config: S3Config) -> duckdb.DuckDBPyConnection:
    """httpfs拡張を設定したDuckDB接続を生成する。

    Lambda環境ではファイルシステムが読み取り専用のため、
    httpfs拡張のインストール先として /tmp を指定する必要がある。
    """
    conn = duckdb.connect(":memory:")
    conn.execute("SET home_directory='/tmp'")
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_region='{s3_config.region}'")
    conn.execute(f"SET s3_access_key_id='{s3_config.access_key_id}'")
    conn.execute(f"SET s3_secret_access_key='{s3_config.secret_access_key}'")
    if s3_config.session_token:
        conn.execute(f"SET s3_session_token='{s3_config.session_token}'")
    if s3_config.endpoint:
        conn.execute(f"SET s3_endpoint='{s3_config.endpoint}'")
    conn.execute(f"SET s3_use_ssl={'true' if s3_config.use_ssl else 'false'}")
    conn.execute(f"SET s3_url_style='{s3_config.url_style}'")
    return conn


def read_tsv_from_s3_with_duckdb(s3_config: S3Config, bucket: str, key: str) -> pa.Table:
    """DuckDB httpfsを使ってS3上のTSVを直接読み込みPyArrow Tableとして返す。

    /tmp へのダウンロードを経由せず、DuckDBがS3へ直接アクセスする。
    """
    conn = _create_duckdb_connection(s3_config)
    s3_path = f"s3://{bucket}/{key}"
    logger.info(f"Reading {s3_path} via DuckDB httpfs")
    query = f"SELECT * FROM read_csv('{s3_path}', delim='\t', header=true)"
    return conn.execute(query).fetch_arrow_table()


def load_to_iceberg(
    catalog: Catalog,
    namespace: str,
    table_name: str,
    arrow_table: pa.Table,
) -> None:
    """Icebergテーブルに対して全件削除→Appendを行う（洗い替え）。

    overwrite(AlwaysTrue()) により既存データの全削除と新規データの追記を
    1トランザクションで実行する。途中エラー時もIcebergのトランザクション管理に
    より一貫した状態が保たれる。
    """
    iceberg_table = catalog.load_table(f"{namespace}.{table_name}")
    logger.info(f"Overwriting {namespace}.{table_name} with {arrow_table.num_rows} rows")
    iceberg_table.overwrite(arrow_table, overwrite_filter=AlwaysTrue())


def load_tsv_to_iceberg(
    s3_config: S3Config,
    catalog: Catalog,
    namespace: str,
    table_name: str,
    bucket: str,
    key: str,
) -> None:
    """S3上のTSVファイルをIcebergテーブルにロードするメイン処理。

    Args:
        s3_config: DuckDB httpfs用のS3接続設定
        catalog: PyIceberg Catalog（GlueCatalog or SqlCatalog）
        namespace: Icebergネームスペース（Glueデータベース名）
        table_name: Icebergテーブル名
        bucket: 入力TSVのS3バケット名
        key: 入力TSVのS3オブジェクトキー
    """
    try:
        arrow_table = read_tsv_from_s3_with_duckdb(s3_config, bucket, key)

        if arrow_table.num_rows == 0:
            logger.warning(f"s3://{bucket}/{key} has 0 rows. Skipping load.")
            return

        load_to_iceberg(catalog, namespace, table_name, arrow_table)
        logger.info(
            f"Successfully loaded {arrow_table.num_rows} rows "
            f"from s3://{bucket}/{key} to {namespace}.{table_name}"
        )

    except Exception as e:
        logger.error(f"Failed to load TSV to Iceberg: {e}")
        raise
