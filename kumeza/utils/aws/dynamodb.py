import logging
from typing import Union

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class InvalidDynamoDBPutItemOperation(Exception):
    def __init__(self, message="Failure in DynamoDB PutItem Operation"):
        self.message = message
        super().__init__(self.message)


class DynamoDB(BaseAwsUtil):
    def __init__(self, region_name: str = "eu-west-1"):
        super().__init__(service_name="dynamodb", region_name=region_name)

    @boto_error_handler(logger)
    def put_item(self, item: dict, table_name: str):
        logger.info("Putting item into %s", table_name)
        dynamojson = self.python_to_dynamo_json(item)
        try:
            resp = self._create_boto_client().put_item(
                TableName=table_name, Item=dynamojson
            )
            return resp["ResponseMetadata"]["HTTPStatusCode"]
        except ClientError as e:
            logger.info(
                "Operation failed with code: %s",
                e.response["Error"]["Code"],
            )
            raise InvalidDynamoDBPutItemOperation from e

    @boto_error_handler(logger)
    def get_item(self, keys: dict, table_name: str):
        logger.info("Get item from %s using %s", table_name, keys)
        keys_in_ddb_json = self.python_to_dynamo_json(keys)
        return self._create_boto_client().get_item(
            TableName=table_name, Key=keys_in_ddb_json
        )

    @boto_error_handler(logger)
    def get_last_item_from_table(
        self,
        table_name: str,
        item_name: str,
        partition_key: str,
        sort_key: str,
        current_epoch: str,
    ):
        # Limitation: Primary Key and Sort Key of
        # the Table should be defined as String Type
        logger.info(
            "Table name: %s | Item name: %s | Primary key: %s \
                | Sort key: %s | Current epoch: %s",
            table_name,
            item_name,
            partition_key,
            sort_key,
            current_epoch,
        )
        try:
            resp = self._create_boto_client().query(
                TableName=table_name,
                KeyConditionExpression=(
                    f"""{partition_key} = :{partition_key} """
                    f"""AND {sort_key} <= :{sort_key}"""
                ),
                ExpressionAttributeValues={
                    f":{partition_key}": {"S": f"{item_name}"},
                    f":{sort_key}": {"S": f"{current_epoch}"},
                },
                ScanIndexForward=False,
                Limit=1,
            )
            return resp
        except ClientError as e:
            logger.info(
                "Error when getting last execution: %s",
                {e.response["Error"]["Message"]},
            )
            raise e

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
