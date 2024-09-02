import json
import logging
from io import BytesIO, StringIO
from typing import Any, Union

import pyarrow as pa
import pyarrow.parquet as pq

from kumeza.utils import ProgressPercentage
from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class S3(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="s3", region_name="eu-west-1")

    @boto_error_handler(logger)
    def write_to_bucket(
        self,
        content: Union[bytes, StringIO, BytesIO, list[dict], pa.Table],
        bucket_name: str = "",
        key_name: str = "",
    ):
        logger.info(
            "Writing content into bucket: %s, and key: %s", bucket_name, key_name
        )

        def write_table_to_parquet(content):
            logger.info("Writing content as Parquet file")
            writer = pa.BufferOutputStream()
            pq.write_table(content, writer)
            body = bytes(writer.getvalue())
            return body

        body: Any = None

        # If content is arrow table
        if isinstance(content, pa.Table):
            body = write_table_to_parquet(content)
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
        logger.info("Getting object from %s with key %s", bucket_name, key_name)
        return self._create_boto_client().get_object(Bucket=bucket_name, Key=key_name)

    @boto_error_handler(logger)
    def upload_file(
        self, file_name: str = "", bucket_name: str = "", object_name: str = ""
    ):
        logger.info(
            "Uploading object: %s into: %s with name: %s",
            file_name,
            bucket_name,
            object_name,
        )
        return self._create_boto_client().upload_file(
            file_name, bucket_name, object_name, Callback=ProgressPercentage(file_name)
        )

    @boto_error_handler(logger)
    def download_file(
        self, bucket_name: str = "", object_name: str = "", file_name: str = "'"
    ):
        logger.info(
            "Downloading %s from %s as: %s", object_name, bucket_name, file_name
        )
        return self._create_boto_client().download_file(
            bucket_name, object_name, file_name
        )
