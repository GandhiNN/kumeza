from kumeza.utils.aws import BaseAwsUtil


class S3(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="s3", region_name="eu-west-1")

    def write_buffer(self, buf, bucket_name: str = "", key_name: str = ""):
        return self._create_boto_client().put_object(
            Body=buf.getvalue(),
            Bucket=bucket_name,
            Key=key_name,
        )

    def get_object(self, bucket_name: str = "", key_name: str = ""):
        return self._create_boto_client().get_object(Bucket=bucket_name, Key=key_name)

    def upload_file(self, file_name, bucket_name, object_name):
        if object_name == None:
            object_name = file_name
        return self._create_boto_client().upload_file(
            file_name, bucket_name, object_name
        )

    def download_file(self, file_name, bucket_name, object_name):
        if file_name == None:
            file_name = object_name
        return self._create_boto_client().download_file(
            bucket_name, object_name, file_name
        )
