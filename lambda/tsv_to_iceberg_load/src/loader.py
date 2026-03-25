import logging
import os

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import AlwaysTrue

logger = logging.getLogger(__name__)


def download_from_s3(s3_client, bucket: str, key: str, local_path: str) -> None:
    """S3からTSVファイルをローカルにダウンロードする"""
    logger.info(f"Downloading s3://{bucket}/{key} to {local_path}")
    s3_client.download_file(bucket, key, local_path)


def read_tsv_with_duckdb(local_path: str) -> pa.Table:
    """DuckDBを使ってローカルのTSVファイルを読み込みPyArrow Tableとして返す"""
    conn = duckdb.connect(":memory:")
    query = f"SELECT * FROM read_csv('{local_path}', delim='\t', header=true)"
    return conn.execute(query).fetch_arrow_table()


def load_to_iceberg(
    catalog: Catalog,
    namespace: str,
    table_name: str,
    arrow_table: pa.Table,
) -> None:
    """Icebergテーブルに対して全件削除→Appendを行う（洗い替え）

    overwrite(AlwaysTrue()) により既存データの全削除と新規データの追記を
    1トランザクションで実行する。途中エラー時もIcebergのトランザクション管理に
    より一貫した状態が保たれる。
    """
    iceberg_table = catalog.load_table(f"{namespace}.{table_name}")
    logger.info(f"Overwriting {namespace}.{table_name} with {arrow_table.num_rows} rows")
    iceberg_table.overwrite(arrow_table, overwrite_filter=AlwaysTrue())


def load_tsv_to_iceberg(
    s3_client,
    catalog: Catalog,
    namespace: str,
    table_name: str,
    bucket: str,
    key: str,
    tmp_dir: str = "/tmp",
) -> None:
    """S3上のTSVファイルをIcebergテーブルにロードするメイン処理

    Args:
        s3_client: boto3 S3クライアント
        catalog: PyIceberg Catalog（GlueCatalog or SqlCatalog）
        namespace: Icebergネームスペース（Glueデータベース名）
        table_name: Icebergテーブル名
        bucket: 入力TSVのS3バケット名
        key: 入力TSVのS3オブジェクトキー
        tmp_dir: ローカル一時ディレクトリ
    """
    local_path = os.path.join(tmp_dir, os.path.basename(key))

    try:
        download_from_s3(s3_client, bucket, key, local_path)

        arrow_table = read_tsv_with_duckdb(local_path)

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

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
