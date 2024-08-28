import json
import logging
from io import BytesIO, StringIO
from typing import Any, Union

from kumeza.utils import ProgressPercentage
from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class S3(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="s3", region_name="eu-west-1")

    @boto_error_handler(logger)
    def write_to_bucket(
        self,
        content: Union[bytes, StringIO, BytesIO, list[dict]],
        bucket_name: str = "",
        key_name: str = "",
    ):
        body: Any = None
        if isinstance(content, (BytesIO, StringIO)):
            body = content.getvalue()
        elif isinstance(content, bytes):
            body = content
        else:
            body = json.dumps(content, indent=4, default=str)
        return self._create_boto_client().put_object(
            Body=body,
            Bucket=bucket_name,
            Key=key_name,
        )

    @boto_error_handler(logger)
    def get_object(self, bucket_name: str = "", key_name: str = ""):
        return self._create_boto_client().get_object(Bucket=bucket_name, Key=key_name)

    @boto_error_handler(logger)
    def upload_file(
        self, file_name: str = "", bucket_name: str = "", object_name: str = ""
    ):
        return self._create_boto_client().upload_file(
            file_name, bucket_name, object_name, Callback=ProgressPercentage(file_name)
        )

    @boto_error_handler(logger)
    def download_file(
        self, bucket_name: str = "", object_name: str = "", file_name: str = "'"
    ):
        return self._create_boto_client().download_file(
            bucket_name, object_name, file_name
        )
