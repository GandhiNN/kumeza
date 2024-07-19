import logging
from typing import Dict

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class STS(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="sts", region_name="eu-west-1")

    @boto_error_handler(logger)
    def assume_role(self, assume_role_arn: str = "") -> Dict:
        return self._create_boto_client().assume_role(
            RoleArn=assume_role_arn, RoleSessionName="assume-role-session"
        )
