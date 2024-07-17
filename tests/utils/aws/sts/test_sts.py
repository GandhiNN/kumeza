# pylint: disable=redefined-outer-name
#
import json

import boto3
import pytest
from moto import mock_aws

from kumeza.utils.aws.sts.sts import STS


ACCOUNT_ID = "icloud"
ROLE_NAME = "test_role"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/{ROLE_NAME}"


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
    trust_policy_document = {
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT_ID}:root"},
            "Action": "sts:AssumeRole",
        },
    }
    create_mocked_iam_connection.create_role(
        RoleName=ROLE_ARN, AssumeRolePolicyDocument=json.dumps(trust_policy_document)
    )


def test_retrieve_assume_role(
    monkeypatch, create_mocked_sts_connection, create_mocked_iam_policy
):
    def get_mocked_sts_assume_role(*args, **kwargs):
        return create_mocked_sts_connection

    monkeypatch.setattr(STS, "_create_boto_client", get_mocked_sts_assume_role)

    sts_client = STS()
    resp = sts_client.assume_role(assume_role_arn=ROLE_ARN)
    credentials = resp["Credentials"]

    assert credentials["AccessKeyId"].startswith("ASIA")
    assert len(credentials["SessionToken"]) == 356
    assert resp["AssumedRoleUser"]["Arn"].endswith(f"/{ROLE_NAME}/assume-role-session")
