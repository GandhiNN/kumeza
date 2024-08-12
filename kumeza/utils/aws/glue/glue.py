import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler
from kumeza.utils.aws.s3.s3 import S3


logger = logging.getLogger(__name__)


class Glue(BaseAwsUtil):
    def __init__(self):
        super().__init__(service_name="glue", region_name="eu-west-1")

    @boto_error_handler(logger)
    def start_glue_job(self, glue_job_name: str, args: dict):
        return self._create_boto_client().start_job_run(
            JobName=glue_job_name, Arguments=args
        )


class GluePythonShell(Glue):

    def get_pyshell_referenced_files(
        self, bucket_name: str, object_name: str, file_name: str, **paramfile_opt: dict
    ):  # pragma: no cover
        s3 = S3()
        s3.download_file(
            bucket_name=paramfile_opt[bucket_name],
            object_name=paramfile_opt[object_name],
            file_name=file_name,
        )
