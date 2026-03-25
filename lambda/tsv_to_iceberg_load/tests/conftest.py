import boto3
import pytest
from moto import mock_aws
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

# テスト用テーブルスキーマ（本番テーブル定義に合わせて変更すること）
TEST_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "value", StringType(), required=False),
)

TEST_NAMESPACE = "bronze"
TEST_TABLE = "test_table"
TEST_BUCKET = "test-input-bucket"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="ap-northeast-1")
        client.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ap-northeast-1"},
        )
        yield client


@pytest.fixture
def local_catalog(tmp_path):
    catalog = SqlCatalog(
        "local",
        **{
            "uri": f"sqlite:///{tmp_path}/iceberg.db",
            "warehouse": f"file://{tmp_path}/warehouse",
        },
    )
    catalog.create_namespace(TEST_NAMESPACE)
    catalog.create_table(
        identifier=f"{TEST_NAMESPACE}.{TEST_TABLE}",
        schema=TEST_SCHEMA,
    )
    yield catalog
