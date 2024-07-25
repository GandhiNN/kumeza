# pylint: disable=redefined-outer-name
#
import json
import unittest

import boto3
from moto import mock_aws

from kumeza.utils.aws.sts.sts import STS


ACCOUNT_ID = "icloud"
ROLE_NAME = "test_role"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/{ROLE_NAME}"


class STSTestIntegration(unittest.TestCase):

    @mock_aws
    def setUp(self):
        # Setup the mock connection for both sts and iam
        self.sts_client = boto3.session.Session().client(
            service_name="sts", region_name="eu-west-1"
        )
        self.iam_client = boto3.session.Session().client(
            service_name="iam", region_name="eu-west-1"
        )

    @mock_aws
    def create_mocked_iam_policy(self):
        trust_policy_document = {
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT_ID}:root"},
                "Action": "sts:AssumeRole",
            },
        }
        self.iam_client.create_role(
            RoleName=ROLE_ARN,
            AssumeRolePolicyDocument=json.dumps(trust_policy_document),
        )

    @mock_aws
    def test_retrieve_assume_role(self):
        # Create the mocked IAM
        self.create_mocked_iam_policy()

        # Create sts client with mocked properties
        sts_client = STS()
        resp = sts_client.assume_role(assume_role_arn=ROLE_ARN)
        credentials = resp["Credentials"]

        assert credentials["AccessKeyId"].startswith("ASIA")
        assert len(credentials["SessionToken"]) == 356
        assert resp["AssumedRoleUser"]["Arn"].endswith(
            f"/{ROLE_NAME}/assume-role-session"
        )
