import logging
import os

from src.clients import create_glue_catalog, create_s3_client
from src.loader import load_tsv_to_iceberg

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    region = os.environ["GLUE_REGION"]
    database = os.environ["GLUE_DATABASE"]
    table = os.environ["GLUE_TABLE"]

    bucket: str = event["s3_bucket"]
    key: str = event["s3_key"]

    s3_client = create_s3_client()
    catalog = create_glue_catalog(name=database, region=region)

    load_tsv_to_iceberg(
        s3_client=s3_client,
        catalog=catalog,
        namespace=database,
        table_name=table,
        bucket=bucket,
        key=key,
    )

    return {"statusCode": 200, "body": "OK"}
