import pyarrow as pa
import pytest

from src.loader import (
    load_to_iceberg,
    load_tsv_to_iceberg,
    read_tsv_from_s3_with_duckdb,
)
from tests.conftest import TEST_BUCKET, TEST_NAMESPACE, TEST_TABLE


# ---------------------------------------------------------------------------
# read_tsv_from_s3_with_duckdb
# ---------------------------------------------------------------------------


def test_read_tsv_from_s3_with_duckdb_returns_arrow_table(s3_client, s3_config):
    """S3上のTSVをDuckDB httpfsで直接読み込みPyArrow Tableとして返すこと"""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key="data/input.tsv",
        Body="id\tname\tvalue\n1\talice\t100\n2\tbob\t200\n".encode(),
    )

    result = read_tsv_from_s3_with_duckdb(s3_config, TEST_BUCKET, "data/input.tsv")

    assert isinstance(result, pa.Table)
    assert result.num_rows == 2
    assert result.column_names == ["id", "name", "value"]


def test_read_tsv_from_s3_with_duckdb_empty_file_returns_empty_table(s3_client, s3_config):
    """ヘッダーのみのTSVファイルは0件のTableを返すこと"""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key="data/empty.tsv",
        Body="id\tname\tvalue\n".encode(),
    )

    result = read_tsv_from_s3_with_duckdb(s3_config, TEST_BUCKET, "data/empty.tsv")

    assert result.num_rows == 0


def test_read_tsv_from_s3_with_duckdb_raises_when_key_not_found(s3_client, s3_config):
    """存在しないS3キーを指定した場合に例外が発生すること"""
    with pytest.raises(Exception):
        read_tsv_from_s3_with_duckdb(s3_config, TEST_BUCKET, "data/notexist.tsv")


# ---------------------------------------------------------------------------
# load_to_iceberg
# ---------------------------------------------------------------------------


def test_load_to_iceberg_appends_data(local_catalog):
    """Icebergテーブルにデータがロードされること"""
    arrow_table = pa.table({"id": [1, 2], "name": ["alice", "bob"], "value": ["100", "200"]})

    load_to_iceberg(local_catalog, TEST_NAMESPACE, TEST_TABLE, arrow_table)

    result = local_catalog.load_table(f"{TEST_NAMESPACE}.{TEST_TABLE}").scan().to_arrow()
    assert result.num_rows == 2


def test_load_to_iceberg_overwrites_existing_data(local_catalog):
    """2回実行しても既存データが削除されてデータが重複しないこと（洗い替え）"""
    first = pa.table({"id": [1, 2], "name": ["alice", "bob"], "value": ["100", "200"]})
    second = pa.table({"id": [3], "name": ["carol"], "value": ["300"]})

    load_to_iceberg(local_catalog, TEST_NAMESPACE, TEST_TABLE, first)
    load_to_iceberg(local_catalog, TEST_NAMESPACE, TEST_TABLE, second)

    result = local_catalog.load_table(f"{TEST_NAMESPACE}.{TEST_TABLE}").scan().to_arrow()
    assert result.num_rows == 1
    assert result["id"][0].as_py() == 3


def test_load_to_iceberg_raises_when_table_not_found(local_catalog):
    """存在しないテーブルを指定した場合に例外が発生すること"""
    arrow_table = pa.table({"id": [1], "name": ["alice"], "value": ["100"]})

    with pytest.raises(Exception):
        load_to_iceberg(local_catalog, TEST_NAMESPACE, "nonexistent_table", arrow_table)


# ---------------------------------------------------------------------------
# load_tsv_to_iceberg（統合テスト）
# ---------------------------------------------------------------------------


def test_load_tsv_to_iceberg_success(s3_client, s3_config, local_catalog):
    """S3のTSVをIcebergテーブルに正常ロードできること"""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key="data/input.tsv",
        Body="id\tname\tvalue\n1\talice\t100\n2\tbob\t200\n".encode(),
    )

    load_tsv_to_iceberg(
        s3_config=s3_config,
        catalog=local_catalog,
        namespace=TEST_NAMESPACE,
        table_name=TEST_TABLE,
        bucket=TEST_BUCKET,
        key="data/input.tsv",
    )

    result = local_catalog.load_table(f"{TEST_NAMESPACE}.{TEST_TABLE}").scan().to_arrow()
    assert result.num_rows == 2


def test_load_tsv_to_iceberg_idempotent(s3_client, s3_config, local_catalog):
    """同じファイルを2回ロードしてもデータが重複しないこと"""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key="data/input.tsv",
        Body="id\tname\tvalue\n1\talice\t100\n2\tbob\t200\n".encode(),
    )

    for _ in range(2):
        load_tsv_to_iceberg(
            s3_config=s3_config,
            catalog=local_catalog,
            namespace=TEST_NAMESPACE,
            table_name=TEST_TABLE,
            bucket=TEST_BUCKET,
            key="data/input.tsv",
        )

    result = local_catalog.load_table(f"{TEST_NAMESPACE}.{TEST_TABLE}").scan().to_arrow()
    assert result.num_rows == 2


def test_load_tsv_to_iceberg_skips_empty_file(s3_client, s3_config, local_catalog):
    """0件のTSVファイルはIcebergへの書き込みをスキップすること"""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key="data/empty.tsv",
        Body="id\tname\tvalue\n".encode(),
    )

    load_tsv_to_iceberg(
        s3_config=s3_config,
        catalog=local_catalog,
        namespace=TEST_NAMESPACE,
        table_name=TEST_TABLE,
        bucket=TEST_BUCKET,
        key="data/empty.tsv",
    )

    result = local_catalog.load_table(f"{TEST_NAMESPACE}.{TEST_TABLE}").scan().to_arrow()
    assert result.num_rows == 0


def test_load_tsv_to_iceberg_raises_when_s3_key_not_found(s3_client, s3_config, local_catalog):
    """S3ファイルが存在しない場合に例外が発生してLambdaが異常終了すること"""
    with pytest.raises(Exception):
        load_tsv_to_iceberg(
            s3_config=s3_config,
            catalog=local_catalog,
            namespace=TEST_NAMESPACE,
            table_name=TEST_TABLE,
            bucket=TEST_BUCKET,
            key="data/notexist.tsv",
        )
