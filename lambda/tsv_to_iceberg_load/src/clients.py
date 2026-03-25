import boto3
from pyiceberg.catalog.glue import GlueCatalog


def create_s3_client():
    return boto3.client("s3")


def create_glue_catalog(name: str, region: str) -> GlueCatalog:
    return GlueCatalog(name, **{"region_name": region})
