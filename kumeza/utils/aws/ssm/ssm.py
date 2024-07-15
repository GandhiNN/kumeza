import json
import logging

import boto3
from botocore.exceptions import ClientError


log = logging.getLogger(__name__)


class SSM:
    def __init__(self):
        self.session = None
        self.client = None

    def get_secret(self, secret_name: str = "", region_name: str = "eu-west-1"):
        self.session = boto3.session.Session()
        self.client = self.session.client(
            service_name="secretsmanager", region_name=region_name
        )
        try:
            resp = self.client.get_secret_value(SecretId=secret_name)
            secret: str = ""
            if "SecretString" in resp:
                secret = resp["SecretString"]
            else:
                secret = resp["SecretBinary"]
            secret_json = json.loads(secret)
        except ClientError as e:
            error_resp = e.response["Error"]["Code"]
            if error_resp == "ResourceNotFoundException":
                # TODO
                log.error(e)
            elif error_resp == "InvalidRequestException":
                # TODO
                log.error(e)
            elif error_resp == "DecryptionFailure":
                # TODO
                log.error(e)
            elif error_resp == "InternalServiceError":
                # TODO
                log.error(e)
        return secret_json
