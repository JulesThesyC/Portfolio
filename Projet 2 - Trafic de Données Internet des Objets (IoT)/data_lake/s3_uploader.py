"""
Data Lake - Upload S3
---------------------
Charge les données vers AWS S3 (optionnel).
"""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd

from config import AWS_S3_BUCKET, AWS_REGION, DATA_LAKE


def upload_to_s3(df: pd.DataFrame, key_prefix: str = "iot/sensors") -> str:
    """
    Upload les données vers S3.
    Nécessite boto3 et credentials AWS configurées.
    """
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 requis : pip install boto3")

    now = datetime.utcnow()
    key = f"{key_prefix}/{now.year}/{now.month:02d}/{now.day:02d}/sensors_{now.strftime('%H%M%S')}.parquet"

    buffer = df.to_parquet(index=False)
    client = boto3.client("s3", region_name=AWS_REGION)
    client.put_object(Bucket=AWS_S3_BUCKET, Key=key, Body=buffer)

    return f"s3://{AWS_S3_BUCKET}/{key}"


def upload_file_to_s3(local_path: Path) -> str:
    """Upload un fichier Parquet local vers S3."""
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 requis : pip install boto3")

    key = f"iot/sensors/{local_path.name}"
    client = boto3.client("s3", region_name=AWS_REGION)
    client.upload_file(str(local_path), AWS_S3_BUCKET, key)
    return f"s3://{AWS_S3_BUCKET}/{key}"
