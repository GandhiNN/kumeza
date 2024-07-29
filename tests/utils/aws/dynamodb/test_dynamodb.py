# pylint: disable-redefined-outer-name
#
import json
import os
import unittest

import boto3
import boto3.session
from moto import mock_aws

from kumeza.utils.aws.dynamodb.dynamodb import DynamoDB


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
    "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
}
PARTITION_KEY = "pipeline_name"
SORT_KEY = "execution_time"


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

        assert result_put["ResponseMetadata"]["HTTPStatusCode"] == 200

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
        


