# pylint: disable-redefined-outer-name
#
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
        {"AttributeName": "partition_key", "AttributeType": "S"},
        {"AttributeName": "sort_key", "AttributeType": "S"},
    ],
    "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
}


@pytest.fixture
def create_mocked_dynamodb_connection():
    with mock_aws():
        yield boto3.session.Session().client(
            service_name="dynamodb", region_name="eu-west-1"
        )


@pytest.fixture
def create_mocked_dynamodb_table(create_mocked_dynamodb_connection):
    create_mocked_dynamodb_connection.create_table(PARAMS)


def test_write_item(
    monkeypatch, create_mocked_dynamodb_connection, create_mocked_dynamodb_table
):
    pass
