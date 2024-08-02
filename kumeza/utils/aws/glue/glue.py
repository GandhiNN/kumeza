import logging
import sys

from awsglue.utils import getResolvedOptions

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class Glue(BaseAwsUtil):
    def __init__(self):
        super().__init__(service_name="glue", region_name="eu-west-1")

    @boto_error_handler(logger)
    def start_glue_job(self, glue_job_name: str, args: dict):
        # glue_args = getResolvedOptions(sys.argv, args)
        self._create_boto_client().start_job_run(JobName=glue_job_name, Arguments=args)

    @boto_error_handler(logger)
    def create_glue_job(
        self, glue_job_name: str, glue_job_role: str, glue_commands: dict
    ):
        return self._create_boto_client().create_job(
            Name=glue_job_name, Role=glue_job_role, Command=glue_commands
        )
