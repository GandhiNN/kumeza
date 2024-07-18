import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler

log = logging.getLogger(__name__)

class DynamoDB(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="dynamodb", region_name="eu-west-1")

    def write_table(self):
        pass

    def get_table(self):
        pass
    
