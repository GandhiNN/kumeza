import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class DynamoDB(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="dynamodb", region_name="eu-west-1")

    @boto_error_handler(logger)
    def write_item(self, item, table_name):
        pass

    @boto_error_handler(logger)
    def get_item(self, partition_key, sort_key, table_name):
        pass
