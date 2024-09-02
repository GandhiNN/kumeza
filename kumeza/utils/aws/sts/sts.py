import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class STS(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="sts", region_name="eu-west-1")

    @boto_error_handler(logger)
    def assume_role(self, assume_role_arn: str = "") -> dict:
        logger.info("Assuming role: %s", assume_role_arn)
        return self._create_boto_client().assume_role(
            RoleArn=assume_role_arn, RoleSessionName="assume-role-session"
        )
