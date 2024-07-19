import logging

from boto3.dynamodb.types import TypeSerializer

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class DynamoDB(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="dynamodb", region_name="eu-west-1")

    @boto_error_handler(logger)
    def write_item(self, item: dict, table_name: str):
        return self._create_boto_client().put_item(
            TableName=table_name, Item=self._python_to_dynamo_json(item)
        )

    @boto_error_handler(logger)
    def get_item(self, partition_key, sort_key, table_name):
        pass

    @boto_error_handler(logger)
    def _python_to_dynamo_json(self, obj: dict) -> dict:
        serializer = TypeSerializer()
        return {k: serializer.serialize(v) for k, v in obj.items()}
