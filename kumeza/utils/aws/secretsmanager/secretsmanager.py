import json

import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)

class SecretsManager(BaseAwsUtil):
    def __init__(self):
        super().__init__(service_name="secretsmanager", region_name="eu-west-1")
    
    @boto_error_handler(logger)
    def get_secret_value(self, secret_name):
        return json.loads(
            self._create_boto_client().get_secret_value(SecretId=secret_name)[
                "SecretString"
            ]
        )
