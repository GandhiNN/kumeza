import json
import os
import unittest

import boto3
import boto3.session
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from kumeza.utils.aws.dynamodb.dynamodb import DynamoDB, InvalidDynamoDBPutItemOperation
from kumeza.utils.common.date_object import DateObject


# Constants
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
    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
}
PARTITION_KEY = "pipeline_name"
SORT_KEY = "execution_time"

# Date object
date_obj = DateObject()


class DynamoDBTestIntegration(unittest.TestCase):

    @mock_aws
    def setUp(self):
        # Setup the mock connection
        self.dynamodb_client = boto3.session.Session().client(
            service_name="dynamodb", region_name="eu-west-1"
        )

    @mock_aws
    def test_put_and_get_item(self):

        # Create the mock table
        self.dynamodb_client.create_table(**PARAMS)

        # Run test on the function
        dynamodb_client = DynamoDB()

        # Open the local test file
        test_json_file = os.path.join(os.path.dirname(__file__), "item.json")
        with open(test_json_file, "r", encoding="utf8") as json_file:
            python_json = json.load(json_file)

        result_put = dynamodb_client.put_item(python_json, TABLE_NAME)

        assert result_put == 200

        # Test keys
        keys = {
            "pipeline_name": "pipe-doadi-cvqa-product_family",
            "execution_time": "1674019243",
        }
        result_get = dynamodb_client.get_item(keys, TABLE_NAME)
        result_get_deserialized = dynamodb_client.dynamo_to_python_json(
            result_get["Item"]
        )

        assert result_get_deserialized == python_json

    @mock_aws
    def test_failed_to_put_item(self):
        # Simulate failure to put item due to mismatch of input and the required keys
        # DynamoDB client will raise ValidationException
        # Create the mock table
        self.dynamodb_client.create_table(**PARAMS)

        # Run test on the function
        dynamodb_client = DynamoDB()

        # put faulty item into the table
        with pytest.raises(InvalidDynamoDBPutItemOperation):
            faulty_item = {
                "pipeline_name": 1234,
                "faulty_sort_key": "1674019243",
            }
            dynamodb_client.put_item(faulty_item, TABLE_NAME)

    @mock_aws
    def test_get_last_item_from_table(self):

        # Create the mock table
        self.dynamodb_client.create_table(**PARAMS)

        # Run test on the function
        dynamodb_client = DynamoDB()

        # Open the local test file
        test_json_file = os.path.join(os.path.dirname(__file__), "multiple_items.json")
        with open(test_json_file, "r", encoding="utf8") as json_file:
            python_json = json.load(json_file)

        for j in python_json:
            result_put = dynamodb_client.put_item(j, TABLE_NAME)
            assert result_put == 200

        last_item = dynamodb_client.get_last_item_from_table(
            TABLE_NAME,
            "pipe-doadi-cvqa-product_family",
            PARTITION_KEY,
            SORT_KEY,
            date_obj.get_current_timestamp(ts_format="epoch"),
        )["Items"][0]

        expected: dict = {
            "pipeline_name": "pipe-doadi-cvqa-product_family",
            "execution_time": "1674019500",
            "data_load_type": "il",
            "ingestion_status": "success",
            "last_exec_as_date": "2023-01-18 05:25:00",
            "records_processed": 116,
            "schema_hash": "f2bf8c9800f7c4b1d96f62f918c3e6c8be146d58ae12af844569f2758f401a52",
            "table_name": "PRODUCT_FAMILY",
        }

        last_item_deserialized = dynamodb_client.dynamo_to_python_json(last_item)
        assert last_item_deserialized == expected

    @mock_aws
    def test_fail_get_last_item_from_table(self):
        # simulate failure because of unknown table
        dynamodb_client = DynamoDB()
        with pytest.raises(ClientError):
            _ = dynamodb_client.get_last_item_from_table(
                "unknown_table",
                "unknown_item",
                "unknown_partition_key",
                "unknown_sort_key",
                date_obj.get_current_timestamp(ts_format="epoch"),
            )

    @mock_aws
    def test_dynamo_to_python_json(self):

        # Run test on the function
        dynamodb_client = DynamoDB()

        # open the dynamodb json file
        ddb_json = os.path.join(os.path.dirname(__file__), "dynamo.json")
        with open(ddb_json, "r", encoding="utf8") as json_file:
            ddbjson = json.load(json_file)

        # open the python json file
        python_json = os.path.join(os.path.dirname(__file__), "python.json")
        with open(python_json, "r", encoding="utf8") as json_file:
            pjson = json.load(json_file)

        assert dynamodb_client.dynamo_to_python_json(ddbjson) == pjson
