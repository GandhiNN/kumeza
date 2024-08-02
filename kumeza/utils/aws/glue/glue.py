import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class Glue(BaseAwsUtil):
    def __init__(self):
        super().__init__(service_name="glue", region_name="eu-west-1")

    @boto_error_handler(logger)
    def start_glue_job(self, glue_job_name: str, args: dict):
        return self._create_boto_client().start_job_run(
            JobName=glue_job_name, Arguments=args
        )
