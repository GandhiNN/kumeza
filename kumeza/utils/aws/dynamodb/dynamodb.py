import logging
from typing import Union

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class DynamoDB(BaseAwsUtil):

    def __init__(self):
        super().__init__(service_name="dynamodb", region_name="eu-west-1")

    @boto_error_handler(logger)
    def put_item(self, item: dict, table_name: str):
        dynamojson = self._python_to_dynamo_json(item)
        return self._create_boto_client().put_item(
            TableName=table_name, Item=dynamojson
        )

    @boto_error_handler(logger)
    def get_item(self, keys: dict, table_name: str):
        keys_in_ddb_json = self._python_to_dynamo_json(keys)
        return self._create_boto_client().get_item(
            TableName=table_name, Key=keys_in_ddb_json
        )

    def _python_to_dynamo_json(self, obj: dict) -> dict:
        serializer = TypeSerializer()
        return {k: serializer.serialize(v) for k, v in obj.items()}

    def _dynamo_to_python_json(self, obj: dict) -> dict:
        deserializer = TypeDeserializer()

        def serialize_number(number: str) -> Union[float, int]:
            if "." in number:
                return float(number)
            return int(number)

        # monkeypatch _deserialize_n to handle decimal values during deserialization
        setattr(
            TypeDeserializer,
            "_deserialize_n",
            lambda _, number: serialize_number(number),
        )

        return {k: deserializer.deserialize(v) for k, v in obj.items()}
