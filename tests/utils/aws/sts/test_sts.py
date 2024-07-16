# pylint: disable=redefined-outer-name
#
import json

import boto3
import pytest
from moto import mock_aws

from kumeza.utils.aws.sts.sts import STS


ACCOUNT_ID = "icloud"
SESSION_NAME = "session-name"


@pytest.fixture
def create_mocked_iam_connection():
    with mock_aws():
        yield boto3.session.Session().client(
            service_name="iam", region_name="eu-west-1"
        )


@pytest.fixture
def create_mocked_sts_connection():
    with mock_aws():
        yield boto3.session.Session().client(
            service_name="sts", region_name="eu-west-1"
        )


@pytest.fixture
def create_mocked_iam_policy(create_mocked_iam_connection):
    policy = json.dumps(
        {
            "Statement": [
                {
                    "Sid": "Stmt13690092345534",
                    "Action": ["S3:ListBucket"],
                    "Effect": "Allow",
                    "Resource": ["arn:aws:s3:::foobar-tester"],
                }
            ]
        }
    )
    trust_policy_document = {
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT_ID}:root"},
            "Action": "sts:AssumeRole",
        },
    }
    role_name = "test-role"
    create_mocked_iam_connection.create_role(
        RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy_document)
    )


def test_retrieve_assume_role(
    monkeypatch, create_mocked_sts_connection, create_mocked_iam_policy
):
    def get_mocked_sts_assume_role(*args, **kwargs):
        return create_mocked_sts_connection

    monkeypatch.setattr(STS, "_create_boto_client", get_mocked_sts_assume_role)

    sts_client = STS()

    print(sts_client.assume_role)
