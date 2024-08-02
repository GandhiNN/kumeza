# pylint: disable=redefined-outer-name
#
import unittest

import boto3
from moto import mock_aws

from kumeza.utils.aws.glue.glue import Glue


JOBNAME = "testGlueJob"
JOBARGS = {
    "day_partition_key": "partition_0",
    "hour_partition_key": "partition_1",
}
ROLENAME = "testRole"
COMMAND = {"Name": "testCommand"}


class GlueTestIntegration(unittest.TestCase):

    @mock_aws
    def setUp(self):
        # Setup the mock connection
        self.glue_client = boto3.session.Session().client(
            service_name="glue", region_name="eu-west-1"
        )

    @mock_aws
    def test_create_job(self):
        glue_client = Glue()
        response = glue_client.create_glue_job(JOBNAME, ROLENAME, COMMAND)
        assert response["Name"] == "testGlueJob"

    # @mock_aws
    # def test_start_job_run(self):
    #     glue_client = Glue()
    #     response = glue_client.start_glue_job(JOBNAME, JOBARGS)
    #     assert response["JobRunId"]
