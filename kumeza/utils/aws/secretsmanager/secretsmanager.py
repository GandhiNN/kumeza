import json

from kumeza.utils.aws import BaseAwsUtil


class SecretsManager(BaseAwsUtil):
    def __init__(self):
        super().__init__(service_name="secretsmanager", region_name="eu-west-1")

    def get_secret_value(self, secret_name):
        return json.loads(
            self._create_boto_client().get_secret_value(SecretId=secret_name)[
                "SecretString"
            ]
        )
