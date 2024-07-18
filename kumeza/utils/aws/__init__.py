import logging
from functools import wraps

import boto3
from botocore.exceptions import ClientError


log = logging.getLogger(__name__)


class BaseAwsUtil:

    def __init__(self, service_name: str = "", region_name: str = ""):
        self.service_name = service_name
        self.region_name = region_name

    def _open_boto_session(self):
        return boto3.session.Session()

    def _create_boto_client(self):
        return self._open_boto_session().client(
            service_name=self.service_name, region_name=self.region_name
        )



def boto_error_handler(logger):
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
                    logger.exception("You provided an invalid value for a parameter.")
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
