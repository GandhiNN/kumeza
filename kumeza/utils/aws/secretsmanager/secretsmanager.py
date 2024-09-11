import json
import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class SecretsManager(BaseAwsUtil):
    def __init__(self, region_name: str = "eu-west-1"):
        super().__init__(service_name="secretsmanager", region_name=region_name)

    @boto_error_handler(logger)
    def get_secret(self, secret_name):
        logger.info("Retrieving secret from %s", secret_name)
        return json.loads(
            self._create_boto_client().get_secret_value(SecretId=secret_name)[
                "SecretString"
            ]
        )
