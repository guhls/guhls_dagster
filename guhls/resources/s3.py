from dagster import resource
import boto3


class S3ResourceConstructor:
    def __init__(self):
        self.client_s3 = boto3.client('s3')

    def upload_file(self, file, key):
        self.client_s3.upload_fileobj(Bucket="guhls-lake", Fileobj=file, Key=key)


@resource()
def s3_resource():
    try:
        return S3ResourceConstructor
    except Exception:
        raise "Check Credentials in AWS"
