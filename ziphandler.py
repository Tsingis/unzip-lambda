import os
import io
import zipfile
import logging
import boto3
from datetime import datetime


s3 = boto3.client("s3")

logger = logging.getLogger()
if (logger.handlers):
    for handler in logger.handlers:
        logger.removeHandler(handler)

logFormat = "%(asctime)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=logFormat)


def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    logger.info(f"Received zip {key}")
    if (key.lower().endswith(".zip")):
        try:
            unzip_files(bucket, key)
            archive_zip(bucket, key)
        except Exception:
            logger.error(f"Error handling zip {key}")


def unzip_files(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    with io.BytesIO(obj["Body"].read()) as fb:
        fb.seek(0)
        with zipfile.ZipFile(fb, mode="r") as zipf:
            for file in zipf.infolist():
                key = os.path.join(os.path.dirname(key), file.filename)
                logger.info(f"Unzipped file {key}")
                s3.put_object(Bucket=bucket, Key=key,
                              Body=zipf.read(file))


def archive_zip(bucket, key):
    prefix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    archiveFile = f"{prefix}_{os.path.basename(key)}"
    archiveKey = os.path.join("archive", archiveFile)
    source = os.path.join(bucket, key)
    s3.copy_object(Bucket=bucket, CopySource=source, Key=archiveKey)
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info(f"Archived file {archiveKey}")
