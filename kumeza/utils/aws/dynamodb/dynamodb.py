import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


log = logging.getLogger(__name__)


class DynamoDB(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="dynamodb", region_name="eu-west-1")

    def write_item(self, item, table_name):
        pass

    def get_item(self, partition_key, sort_key, table_name):
        pass
