import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler
from kumeza.utils.aws.s3 import S3


logger = logging.getLogger(__name__)


class Glue(BaseAwsUtil):
    def __init__(self, region_name: str = "eu-west-1"):
        super().__init__(service_name="glue", region_name=region_name)

    @boto_error_handler(logger)
    def start_glue_job(self, glue_job_name: str, args: dict):
        logger.info("Starting %s Glue job", glue_job_name)
        return self._create_boto_client().start_job_run(
            JobName=glue_job_name, Arguments=args
        )

    @boto_error_handler(logger)
    def get_pyshell_referenced_files(
        self,
        bucket_param_name: str,
        object_param_name: str,
        file_name: str,
        glue_args: dict,
    ):  # pragma: no cover
        logger.info(
            "Downloading Glue referenced files = %s from %s - %s",
            file_name,
            bucket_param_name,
            object_param_name,
        )
        s3 = S3()
        s3.download_file(
            bucket_name=glue_args[bucket_param_name],
            object_name=glue_args[object_param_name],
            file_name=file_name,
        )
