import os
import io
import zipfile
import boto3
from datetime import datetime

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    if (key.endswith(".zip")):
        try:
            unzip_files(bucket, key)
            archive_zip(bucket, key)
        except Exception as e:
            print(f"Error handling zip {key}. Exception: {e}")
            raise e

def unzip_files(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    with io.BytesIO(obj["Body"].read()) as fb:
        fb.seek(0)
        with zipfile.ZipFile(fb, mode="r") as zipf:
            for file in zipf.infolist():
                filename = os.path.dirname(key) + "/" + file.filename
                s3.put_object(Bucket=bucket, Key=filename, Body=zipf.read(file))

def archive_zip(bucket, key):
    archive_file = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + os.path.basename(key)
    archive_key = "archive/" + archive_file
    source = bucket + "/" + key
    s3.copy_object(Bucket=bucket, CopySource=source, Key=archive_key)
    s3.delete_object(Bucket=bucket, Key=key)