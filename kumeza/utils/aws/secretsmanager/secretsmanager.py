import json
import logging
from functools import wraps

import boto3
from botocore.exceptions import ClientError


log = logging.getLogger(__name__)


class SecretsManager:
    def __init__(self, secret_name, region_name="eu-west-1"):
        self.secret_name = secret_name
        self.region_name = region_name

    def _open_boto_session(self):
        return boto3.session.Session()

    def _create_boto_client(self):
        return self._open_boto_session().client(
            service_name="secretsmanager", region_name=self.region_name
        )

    def boto_error_handler(self, logger):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except ClientError as e:
                    error_code = e.response["Error"]["Code"]
                    if error_code == "DecryptionFailureException":
                        logger.exception(
                            "Secrets Manager can't decrypt the protected secret "
                            "text using the provided KMS key."
                        )
                        raise e

                    elif error_code == "InternalServiceErrorException":
                        logger.exception("An error occured on the server side.")
                        raise e

                    elif error_code == "InvalidParameterException":
                        logger.exception(
                            "You provided an invalid value for a parameter."
                        )
                        raise e

                    elif error_code == "InvalidRequestException":
                        logger.exception(
                            "You provided a parameter value that is not valid "
                            "for the current state of the resource."
                        )
                        raise e

                    elif error_code == "ResourceNotFoundException":
                        logger.info("We can't find the resource that you asked for.")
                        raise e

            return wrapper

        return decorator

    def get_secret_value(self):
        return json.loads(
            self._create_boto_client().get_secret_value(SecretId=self.secret_name)[
                "SecretString"
            ]
        )
