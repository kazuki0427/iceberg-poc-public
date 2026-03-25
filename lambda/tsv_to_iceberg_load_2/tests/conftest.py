import boto3
import pytest
from moto.server import ThreadedMotoServer
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from src.loader import S3Config

# テスト用テーブルスキーマ（本番テーブル定義に合わせて変更すること）
TEST_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "value", StringType(), required=False),
)

TEST_NAMESPACE = "bronze"
TEST_TABLE = "test_table"
TEST_BUCKET = "test-input-bucket"
MOTO_PORT = 5555


@pytest.fixture(scope="session")
def moto_server():
    """motoをHTTPサーバーとして起動する。

    DuckDB httpfsはboto3を経由せずに直接HTTPリクエストを発行するため、
    通常の @mock_aws デコレータではモックが効かない。
    motoをサーバーモードで起動し、DuckDBのS3エンドポイントをそこへ向けることで
    テスト環境でのS3アクセスをモックする。
    """
    server = ThreadedMotoServer(port=MOTO_PORT)
    server.start()
    yield f"localhost:{MOTO_PORT}"
    server.stop()


@pytest.fixture
def s3_client(moto_server):
    """motoサーバーに接続するboto3 S3クライアント。"""
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{moto_server}",
        region_name="ap-northeast-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    client.create_bucket(
        Bucket=TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "ap-northeast-1"},
    )
    yield client
    # テスト後にバケット内オブジェクトとバケットを削除
    objects = client.list_objects_v2(Bucket=TEST_BUCKET).get("Contents", [])
    for obj in objects:
        client.delete_object(Bucket=TEST_BUCKET, Key=obj["Key"])
    client.delete_bucket(Bucket=TEST_BUCKET)


@pytest.fixture
def s3_config(moto_server):
    """DuckDB httpfs用のS3設定。motoサーバーのエンドポイントを指定する。

    url_style='path' はmotoサーバーがpath-style URLのみサポートするために必要。
    """
    return S3Config(
        region="ap-northeast-1",
        access_key_id="test",
        secret_access_key="test",
        endpoint=moto_server,
        use_ssl=False,
        url_style="path",
    )


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
