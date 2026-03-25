import logging
import os

from src.clients import create_glue_catalog, create_s3_config
from src.loader import load_tsv_to_iceberg

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    region = os.environ["GLUE_REGION"]
    database = os.environ["GLUE_DATABASE"]
    table = os.environ["GLUE_TABLE"]

    bucket: str = event["s3_bucket"]
    key: str = event["s3_key"]

    s3_config = create_s3_config(region=region)
    catalog = create_glue_catalog(name=database, region=region)

    load_tsv_to_iceberg(
        s3_config=s3_config,
        catalog=catalog,
        namespace=database,
        table_name=table,
        bucket=bucket,
        key=key,
    )

    return {"statusCode": 200, "body": "OK"}
