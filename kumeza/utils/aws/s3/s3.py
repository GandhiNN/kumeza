import logging

from kumeza.utils import ProgressPercentage
from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


log = logging.getLogger(__name__)


class S3(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="s3", region_name="eu-west-1")

    @boto_error_handler(log)
    def write_buffer(self, buf, bucket_name: str = "", key_name: str = ""):
        return self._create_boto_client().put_object(
            Body=buf.getvalue(),
            Bucket=bucket_name,
            Key=key_name,
        )

    @boto_error_handler(log)
    def get_object(self, bucket_name: str = "", key_name: str = ""):
        return self._create_boto_client().get_object(Bucket=bucket_name, Key=key_name)

    @boto_error_handler(log)
    def upload_file(
        self, file_name: str = "", bucket_name: str = "", object_name: str = ""
    ):
        return self._create_boto_client().upload_file(
            file_name, bucket_name, object_name, Callback=ProgressPercentage(file_name)
        )

    @boto_error_handler(log)
    def download_file(self, file_name, bucket_name, object_name):
        if file_name == None:
            file_name = object_name
        return self._create_boto_client().download_file(
            bucket_name, object_name, file_name
        )
