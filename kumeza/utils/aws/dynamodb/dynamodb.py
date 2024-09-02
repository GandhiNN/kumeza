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
        logger.info("Putting item into %s", table_name)
        dynamojson = self.python_to_dynamo_json(item)
        return self._create_boto_client().put_item(
            TableName=table_name, Item=dynamojson
        )

    @boto_error_handler(logger)
    def get_item(self, keys: dict, table_name: str):
        logger.info("Get item from %s using %s", table_name, keys)
        keys_in_ddb_json = self.python_to_dynamo_json(keys)
        return self._create_boto_client().get_item(
            TableName=table_name, Key=keys_in_ddb_json
        )

    def python_to_dynamo_json(self, obj: dict) -> dict:
        logger.info("Serializing Python object into DynamoDB JSON object")
        serializer = TypeSerializer()
        return {k: serializer.serialize(v) for k, v in obj.items()}

    def dynamo_to_python_json(self, obj: dict) -> dict:
        logger.info("Deserializing DynamoDB JSON object into Python object")
        deserializer = TypeDeserializer()

        def serialize_number(number: str) -> Union[float, int]:
            if "." in number or "e" in number:
                return float(number)
            return int(number)

        # monkeypatch _deserialize_n to handle decimal values during deserialization
        setattr(
            TypeDeserializer,
            "_deserialize_n",
            lambda _, number: serialize_number(number),
        )

        return {k: deserializer.deserialize(v) for k, v in obj.items()}
