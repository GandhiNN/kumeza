# pylint: disable=redefined-outer-name
#
import os
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


def test_upload_file(monkeypatch, create_mocked_s3_connection, create_mocked_s3_bucket):

    def get_mocked_s3_connection(*args, **kwargs):
        return create_mocked_s3_connection

    monkeypatch.setattr(S3, "_create_boto_client", get_mocked_s3_connection)

    # Upload file
    s3_client = S3()
    local_file = os.path.join(os.path.dirname(__file__), "test.txt")
    s3_client.upload_file(local_file, BUCKET_NAME, "test.txt")

    # Read object from the mocked bucket
    data = s3_client.get_object(bucket_name=BUCKET_NAME, key_name="test.txt")
    contents = data["Body"].read().decode("utf8")  # store in bytes buffer

    # Compare the content of local and remote content
    with open(local_file, "r", encoding="utf8") as localfile:
        assert localfile.read() == contents


def test_download_file(
    monkeypatch, create_mocked_s3_connection, create_mocked_s3_bucket
):

    def get_mocked_s3_connection(*args, **kwargs):
        return create_mocked_s3_connection

    monkeypatch.setattr(S3, "_create_boto_client", get_mocked_s3_connection)

    # Read local file content into memory
    local_file = os.path.join(os.path.dirname(__file__), "test.txt")
    with open(local_file, "r", encoding="utf8") as localfile:

        # Upload file to the mocked bukcet
        s3_client = S3()
        s3_client.upload_file(local_file, BUCKET_NAME, "test.txt")

        # Delete local file
        os.remove(local_file)

        # Download the previously uploaded file back into local
        downloaded_file = os.path.join(os.path.dirname(__file__), "test_downloaded.txt")
        s3_client.download_file(
            bucket_name=BUCKET_NAME, object_name="test.txt", file_name=downloaded_file
        )

        with open(downloaded_file, "r", encoding="utf8") as downloadedfile:
            localfile_content = localfile.read()
            downloadedfile_content = downloadedfile.read()
            assert downloadedfile_content == localfile_content

            # Finally, if assertion passed, rewrite the file into "test.txt"
            with open(local_file, "w", encoding="utf8") as f:
                f.write(downloadedfile_content)
                os.remove(downloaded_file)  # delete the downloaded file
