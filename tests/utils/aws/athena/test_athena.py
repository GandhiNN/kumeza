import unittest

import boto3
from moto import mock_aws

from kumeza.utils.aws.athena.athena import Athena


# raise unittest.SkipTest("##TODO")

# Constants
DATABASE_NAME = "TestDatabase"


class AthenaTestIntegration(unittest.TestCase):

    @mock_aws
    def setUp(self):
        # Setup the base connection
        self.athena_client = boto3.session.Session().client(
            service_name="athena", region_name="eu-west-1"
        )

    @mock_aws
    def test_create_database(self):
        # instantiate client
        athena_client = Athena()

        # Create the database
        result = athena_client.create_database(DATABASE_NAME, "s3://mock-path/query")

        # Assert that execution is ok
        http_status_code = result["ResponseMetadata"]["HTTPStatusCode"]
        assert http_status_code == 200

        # Assert that, for the same query execution id
        # we are seeing the same DDL being executed
        # regardless of query status
        status = self.athena_client.get_query_execution(
            QueryExecutionId=result["QueryExecutionId"]
        )
        query_type = status["QueryExecution"]["StatementType"]
        query = status["QueryExecution"]["Query"]
        assert query == "create database TestDatabase"
        assert query_type == "DDL"
