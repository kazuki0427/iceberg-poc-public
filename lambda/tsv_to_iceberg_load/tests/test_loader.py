import os

import pyarrow as pa
import pytest
from botocore.exceptions import ClientError

from src.loader import (
    download_from_s3,
    load_to_iceberg,
    load_tsv_to_iceberg,
    read_tsv_with_duckdb,
)
from tests.conftest import TEST_BUCKET, TEST_NAMESPACE, TEST_TABLE


# ---------------------------------------------------------------------------
# read_tsv_with_duckdb
# ---------------------------------------------------------------------------


def test_read_tsv_with_duckdb_returns_arrow_table(tmp_path):
    """TSVファイルをDuckDBで読み込みPyArrow Tableとして返すこと"""
    tsv = tmp_path / "input.tsv"
    tsv.write_text("id\tname\tvalue\n1\talice\t100\n2\tbob\t200\n")

    result = read_tsv_with_duckdb(str(tsv))

    assert isinstance(result, pa.Table)
    assert result.num_rows == 2
    assert result.column_names == ["id", "name", "value"]


def test_read_tsv_with_duckdb_empty_file_returns_empty_table(tmp_path):
    """ヘッダーのみのTSVファイルは0件のTableを返すこと"""
    tsv = tmp_path / "empty.tsv"
    tsv.write_text("id\tname\tvalue\n")

    result = read_tsv_with_duckdb(str(tsv))

    assert result.num_rows == 0


# ---------------------------------------------------------------------------
# download_from_s3
# ---------------------------------------------------------------------------


def test_download_from_s3_downloads_file(s3_client, tmp_path):
    """S3からファイルをローカルにダウンロードできること"""
    s3_client.put_object(Bucket=TEST_BUCKET, Key="data/input.tsv", Body=b"id\tname\n1\talice\n")
    local_path = str(tmp_path / "input.tsv")

    download_from_s3(s3_client, TEST_BUCKET, "data/input.tsv", local_path)

    assert os.path.exists(local_path)
    with open(local_path) as f:
        assert f.read() == "id\tname\n1\talice\n"


def test_download_from_s3_raises_when_key_not_found(s3_client, tmp_path):
    """存在しないS3キーを指定した場合に例外が発生すること"""
    with pytest.raises(ClientError):
        download_from_s3(s3_client, TEST_BUCKET, "not/exist.tsv", str(tmp_path / "x.tsv"))


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


def test_load_tsv_to_iceberg_success(s3_client, local_catalog, tmp_path):
    """S3のTSVをIcebergテーブルに正常ロードできること"""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key="data/input.tsv",
        Body="id\tname\tvalue\n1\talice\t100\n2\tbob\t200\n".encode(),
    )

    load_tsv_to_iceberg(
        s3_client=s3_client,
        catalog=local_catalog,
        namespace=TEST_NAMESPACE,
        table_name=TEST_TABLE,
        bucket=TEST_BUCKET,
        key="data/input.tsv",
        tmp_dir=str(tmp_path),
    )

    result = local_catalog.load_table(f"{TEST_NAMESPACE}.{TEST_TABLE}").scan().to_arrow()
    assert result.num_rows == 2


def test_load_tsv_to_iceberg_idempotent(s3_client, local_catalog, tmp_path):
    """同じファイルを2回ロードしてもデータが重複しないこと"""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key="data/input.tsv",
        Body="id\tname\tvalue\n1\talice\t100\n2\tbob\t200\n".encode(),
    )

    for _ in range(2):
        load_tsv_to_iceberg(
            s3_client=s3_client,
            catalog=local_catalog,
            namespace=TEST_NAMESPACE,
            table_name=TEST_TABLE,
            bucket=TEST_BUCKET,
            key="data/input.tsv",
            tmp_dir=str(tmp_path),
        )

    result = local_catalog.load_table(f"{TEST_NAMESPACE}.{TEST_TABLE}").scan().to_arrow()
    assert result.num_rows == 2


def test_load_tsv_to_iceberg_skips_empty_file(s3_client, local_catalog, tmp_path):
    """0件のTSVファイルはIcebergへの書き込みをスキップすること"""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key="data/empty.tsv",
        Body="id\tname\tvalue\n".encode(),
    )

    load_tsv_to_iceberg(
        s3_client=s3_client,
        catalog=local_catalog,
        namespace=TEST_NAMESPACE,
        table_name=TEST_TABLE,
        bucket=TEST_BUCKET,
        key="data/empty.tsv",
        tmp_dir=str(tmp_path),
    )

    result = local_catalog.load_table(f"{TEST_NAMESPACE}.{TEST_TABLE}").scan().to_arrow()
    assert result.num_rows == 0


def test_load_tsv_to_iceberg_raises_when_s3_key_not_found(s3_client, local_catalog, tmp_path):
    """S3ファイルが存在しない場合に例外が発生してLambdaが異常終了すること"""
    with pytest.raises(Exception):
        load_tsv_to_iceberg(
            s3_client=s3_client,
            catalog=local_catalog,
            namespace=TEST_NAMESPACE,
            table_name=TEST_TABLE,
            bucket=TEST_BUCKET,
            key="data/notexist.tsv",
            tmp_dir=str(tmp_path),
        )


def test_load_tsv_to_iceberg_cleans_up_tmp_file(s3_client, local_catalog, tmp_path):
    """処理完了後にローカルの一時ファイルが削除されること"""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key="data/input.tsv",
        Body="id\tname\tvalue\n1\talice\t100\n".encode(),
    )

    load_tsv_to_iceberg(
        s3_client=s3_client,
        catalog=local_catalog,
        namespace=TEST_NAMESPACE,
        table_name=TEST_TABLE,
        bucket=TEST_BUCKET,
        key="data/input.tsv",
        tmp_dir=str(tmp_path),
    )

    assert not os.path.exists(str(tmp_path / "input.tsv"))
