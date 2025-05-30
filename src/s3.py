import os
import logging
from datetime import date, datetime
import tempfile

from minio import Minio
from minio.error import S3Error
import pandas as pd

from config import S3_HOST, S3_PORT, S3_ACCESS_KEY, S3_SECRET_KEY

logger = logging.getLogger(__name__)
endpoint = f"{S3_HOST}:{S3_PORT}"
client = Minio(endpoint=endpoint, 
               access_key=S3_ACCESS_KEY,
               secret_key=S3_SECRET_KEY,
               secure=False)


def get_archive_transactions_file_path(
    date: date,
) -> str:
    bucket_name = 'archive-transactions'
    object_name = f"transactions_{date.strftime('%Y-%m')}.parquet"
    make_bucket(bucket_name)
    return f"http://{endpoint}/{bucket_name}/{object_name}"


def get_archive_deliveries_file_path(
    date: date,
) -> str:
    bucket_name = 'archive-deliveries'
    object_name = f"deliveries_{date.strftime('%Y-%m')}.parquet"
    make_bucket(bucket_name)
    return f"http://{endpoint}/{bucket_name}/{object_name}"


def get_archive_df(
    df_name: str,
) -> pd.DataFrame:
    bucket_name = f"archive-df"
    object_name = f"{df_name}.csv"
    try:
        data = client.get_object(bucket_name, object_name)
        df = pd.read_csv(data)
        logger.info(
            "DataFrame '%s' retrieved from bucket '%s'.", 
            df_name, 
            bucket_name
        )
        return df
    except Exception as e:
        logger.error(
            "Failed to retrieve DataFrame '%s' from bucket '%s': %s", 
            df_name, bucket_name, str(e)
        )
        raise RuntimeError(f"Failed to retrieve DataFrame '{df_name}' from bucket '{bucket_name}'.") from e


def upload_archive_df(
    df_name: str,
    df: pd.DataFrame,
):
    bucket_name = f"archive-df"
    object_name = f"{df_name}.csv"
    with tempfile.NamedTemporaryFile(
        mode="w", 
        suffix=".csv", 
        delete=False
    ) as tmp:
        df.to_csv(tmp.name, index=False)
        tmp.flush()
        upload_file(
            bucket_name=bucket_name, 
            file_path=tmp.name, 
            object_name=object_name
        )
    os.remove(tmp.name)


def upload_daily_report(
    file_path: str,
    date: str,
):
    date = datetime.strptime(date, "%Y-%m-%d").date()
    bucket_name = f"daily-reports-{date.year}"
    object_name_pdf = f"daily_report_{date.strftime('%Y-%m-%d')}.pdf"
    upload_file(
        bucket_name=bucket_name, 
        file_path=file_path, 
        object_name=object_name_pdf
    )
    

def upload_monthly_report(
    file_path: str,
    date: str,
):
    date = datetime.strptime(date, "%Y-%m-%d").date()
    bucket_name = f"monthly-reports-{date.year}"
    object_name = f"monthly_report_{date.strftime('%Y-%m')}.pdf"
    upload_file(
        bucket_name=bucket_name, 
        file_path=file_path, 
        object_name=object_name
    )


def upload_file(
    bucket_name: str, 
    file_path: str, 
    object_name: str = None
):
    make_bucket(bucket_name)
    if object_name is None:
        object_name = os.path.basename(file_path)
    with open(file_path, "rb") as file_data:
        client.put_object(
            bucket_name,
            object_name,
            file_data,
            length=-1,
            part_size=10*1024*1024,
        )
    logger.info(
        "File '%s' uploaded to bucket '%s' as '%s'.", 
        file_path, 
        bucket_name, 
        object_name
    )


def make_bucket(
    bucket_name: str,
):
    if not client.bucket_exists(bucket_name):
        try:
            client.make_bucket(bucket_name)
        except S3Error as e:
            if e.code != "BucketAlreadyOwnedByYou":
                raise RuntimeError(
                    f"Failed to create bucket %s: {str(e)}"
                ) from e
        logger.info("Bucket '%s' created.", bucket_name)
    else:
        logger.info("Bucket '%s' already exists.", bucket_name)