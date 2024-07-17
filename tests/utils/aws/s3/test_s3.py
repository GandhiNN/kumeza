# pylint: disable=redefined-outer-name
#
from io import BytesIO

import boto3
import pytest
from moto import mock_aws

from kumeza.utils.aws.s3.s3 import S3


BUCKET_NAME = "testbucket"
KEY_NAME = "testfile"
BODY = "testbody"
METADATA = "testmetadata"


@pytest.fixture
def create_mocked_s3_connection():
    with mock_aws():
        yield boto3.session.Session().client(service_name="s3", region_name="eu-west-1")


@pytest.fixture
def create_mocked_s3_bucket(create_mocked_s3_connection):
    create_mocked_s3_connection.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )


@pytest.fixture
def create_mocked_s3_object(create_mocked_s3_connection):
    create_mocked_s3_connection.put_object(
        Bucket=BUCKET_NAME, Key=KEY_NAME, Body=BODY, Metadata=METADATA
    )


def test_write_buffer(
    monkeypatch, create_mocked_s3_connection, create_mocked_s3_bucket
):
    def get_mocked_s3_connection(*args, **kwargs):
        return create_mocked_s3_connection

    monkeypatch.setattr(S3, "_create_boto_client", get_mocked_s3_connection)

    s3_client = S3()
    data_input = b"some data 123 321"
    buffer = BytesIO(data_input)  # convert bytestring to buffered reader
    resp = s3_client.write_buffer(
        buf=buffer, bucket_name=BUCKET_NAME, key_name=KEY_NAME
    )
    # make sure the write process is successful
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    get_resp = s3_client.get_object(
        bucket_name=BUCKET_NAME, key_name=KEY_NAME
    )  # read the same object from mocked bucket
    # make sure that we get the same bytestream
    assert get_resp["Body"].read() == data_input  # flushed upon first read


def test_upload_file():
    pass


def test_download_file():
    pass
