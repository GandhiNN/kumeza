# pylint: disable-redefined-outer-name
#
import json
import os

import boto3
import boto3.session
import pytest
from moto import mock_aws

from kumeza.utils.aws.dynamodb.dynamodb import DynamoDB


TABLE_NAME = "TestDynamoDBTable"
PARAMS = {
    "TableName": TABLE_NAME,
    "KeySchema": [
        {"AttributeName": "pipeline_name", "KeyType": "HASH"},
        {"AttributeName": "execution_time", "KeyType": "RANGE"},
    ],
    "AttributeDefinitions": [
        {"AttributeName": "pipeline_name", "AttributeType": "S"},
        {"AttributeName": "execution_time", "AttributeType": "S"},
    ],
    "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
}

PARTITION_KEY = "pipeline_name"
SORT_KEY = "execution_time"


@pytest.fixture
def create_mocked_dynamodb_connection():
    with mock_aws():
        yield boto3.session.Session().client(
            service_name="dynamodb", region_name="eu-west-1"
        )


@pytest.fixture
def create_mocked_dynamodb_table(create_mocked_dynamodb_connection):
    create_mocked_dynamodb_connection.create_table(**PARAMS)


def test_write_item(
    monkeypatch, create_mocked_dynamodb_connection, create_mocked_dynamodb_table
):
    def get_mocked_dynamodb_connection(*args, **kwargs):
        return create_mocked_dynamodb_connection

    monkeypatch.setattr(DynamoDB, "_create_boto_client", get_mocked_dynamodb_connection)

    monkeypatch.setattr(DynamoDB, "_create_boto_client", get_mocked_dynamodb_connection)

    # Instantiate the monkeypatched client
    dynamodb_client = DynamoDB()

    test_json_file = os.path.join(os.path.dirname(__file__), "item.json")
    with open(test_json_file, "r", encoding="utf8") as json_file:
        python_json = json.load(json_file)

    result = dynamodb_client.write_item(python_json, TABLE_NAME)

    assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_item(
    monkeypatch, create_mocked_dynamodb_connection, create_mocked_dynamodb_table
):
    def get_mocked_dynamodb_connection(*args, **kwargs):
        return create_mocked_dynamodb_connection

    monkeypatch.setattr(DynamoDB, "_create_boto_client", get_mocked_dynamodb_connection)

    ddb_client = DynamoDB()
    localfile = os.path.join(os.path.dirname(__file__), "tabledata.json")

    monkeypatch.setattr(DynamoDB, "_create_boto_client", get_mocked_dynamodb_connection)

    # Instantiate the monkeypatched client
    dynamodb_client = DynamoDB()

    # TODO
    ITEM_NAME = None
    expected = ""
    assert dynamodb_client.get_item(PARTITION_KEY, SORT_KEY, TABLE_NAME) == expected
