import boto3
from pyiceberg.catalog.glue import GlueCatalog

from src.loader import S3Config


def create_s3_config(region: str) -> S3Config:
    """Lambda実行ロールの認証情報からS3Configを生成する。"""
    session = boto3.Session()
    credentials = session.get_credentials().resolve()
    return S3Config(
        region=region,
        access_key_id=credentials.access_key,
        secret_access_key=credentials.secret_key,
        session_token=credentials.token,
    )


def create_glue_catalog(name: str, region: str) -> GlueCatalog:
    return GlueCatalog(name, **{"region_name": region})
