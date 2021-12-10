import boto3
import logging
import os
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile


ARCHIVE_ENABLED = True
ARCHIVE_LOCATION = "archive"

s3 = boto3.client("s3")

logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)

log_format = "%(asctime)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)


def handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    logger.info(f"Received zip {key}")
    if key.lower().endswith(".zip"):
        try:
            unzip_files(bucket, key)
            if ARCHIVE_ENABLED:
                archive_zip(bucket, key)
        except Exception:
            logger.exception(f"Error handling zip {key}")


def unzip_files(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    with BytesIO(obj["Body"].read()) as fb:
        fb.seek(0)
        with ZipFile(fb, mode="r") as zipf:
            for file in zipf.infolist():
                key = os.path.join(os.path.dirname(key), file.filename)
                logger.info(f"Unzipped file {key}")
                s3.put_object(Bucket=bucket, Key=key, Body=zipf.read(file))


def archive_zip(bucket, key):
    prefix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    archive_file = f"{prefix}_{os.path.basename(key)}"
    archive_key = os.path.join(ARCHIVE_LOCATION, archive_file)
    source = os.path.join(bucket, key)
    s3.copy_object(Bucket=bucket, CopySource=source, Key=archive_key)
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info(f"Archived file {archive_key}")
