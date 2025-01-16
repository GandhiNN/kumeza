# pylint: disable=redefined-outer-name
#
import unittest

import boto3
from moto import mock_aws

from kumeza.utils.aws.glue import Glue


JOBNAME = "testGlueJob"
JOBARGS = {
    "--assume_role_arn": "someTestARN",
    "--vault_url": "https://vault-url.test",
    "--table_name": "someTestTable",
}
ROLENAME = "testRole"
COMMAND = {"Name": "testCommand"}

PIPELINE_ARGS = ["assume-role-arn", "vault-url", "table-name"]


class GlueTestIntegration(unittest.TestCase):
    @mock_aws
    def setUp(self):
        # Setup the mock connection
        self.glue_client = boto3.session.Session().client(
            service_name="glue", region_name="eu-west-1"
        )

    @mock_aws
    def test_create_and_start_job_run(self):
        resp = self.glue_client.create_job(
            Name=JOBNAME,
            Role=ROLENAME,
            Command=COMMAND,
            DefaultArguments=JOBARGS,
        )
        assert resp["Name"] == "testGlueJob"

        glue_client = Glue()
        start_job_response = glue_client.start_glue_job(JOBNAME, JOBARGS)
        assert start_job_response["JobRunId"]

    @unittest.skip("no way to test this for now")
    def test_get_pyshell_referenced_files(self):
        pass
