# pylint: disable=redefined-outer-name
#
import json
import os
import unittest
from io import BytesIO

import boto3
from moto import mock_aws

from kumeza.utils.aws.s3.s3 import S3


# Constants
BUCKET_NAME = "testbucket"
KEY_NAME = "testfile"
BODY = "testbody"
METADATA = "testmetadata"


class S3TestIntegration(unittest.TestCase):

    @mock_aws
    def setUp(self):
        # Setup the mock connection
        self.s3_client = boto3.session.Session().client(
            service_name="s3", region_name="eu-west-1"
        )

    @mock_aws
    def test_write_bytes_io_to_bucket(self):

        # Create a mock bucket
        self.s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        s3_client = S3()
        data_input = b"some data 123 321"
        buffer = BytesIO(data_input)  # convert bytestring to buffered reader
        resp = s3_client.write_to_bucket(
            content=buffer, bucket_name=BUCKET_NAME, key_name=KEY_NAME
        )
        # make sure the write process is successful
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_resp = s3_client.get_object(
            bucket_name=BUCKET_NAME, key_name=KEY_NAME
        )  # read the same object from mocked bucket
        # make sure that we get the same bytestream
        assert get_resp["Body"].read() == data_input  # flushed upon first read

    @mock_aws
    def test_write_bytes_buffer_to_bucket(self):

        # Create a mock bucket
        self.s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        s3_client = S3()
        data_input = b"some data 123 321"
        resp = s3_client.write_to_bucket(
            content=data_input, bucket_name=BUCKET_NAME, key_name=KEY_NAME
        )
        # make sure the write process is successful
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_resp = s3_client.get_object(
            bucket_name=BUCKET_NAME, key_name=KEY_NAME
        )  # read the same object from mocked bucket
        # make sure that we get the same bytestream
        assert get_resp["Body"].read() == data_input  # flushed upon first read

    @mock_aws
    def test_write_list_of_dict_to_bucket(self):

        # Create a mock bucket
        self.s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        s3_client = S3()
        data_input = [
            {
                "name": "ActionFlagType",
                "type": "int",
                "description": "None",
                "nullable": "True",
            },
            {
                "name": "TextID",
                "type": "int",
                "description": "None",
                "nullable": "True",
            },
        ]
        resp = s3_client.write_to_bucket(
            content=data_input, bucket_name=BUCKET_NAME, key_name=KEY_NAME
        )
        # make sure the write process is successful
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_resp = s3_client.get_object(
            bucket_name=BUCKET_NAME, key_name=KEY_NAME
        )  # read the same object from mocked bucket
        # make sure that we get the same bytestream
        resp_body = get_resp["Body"].read()
        assert json.loads(resp_body.decode("utf-8").replace("'", '"')) == data_input

    @mock_aws
    def test_upload_file(self):

        # Create a mock bucket
        self.s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
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

    @mock_aws
    def test_download_file(self):

        # Create a mock bucket
        self.s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        # Read local file content into memory
        local_file = os.path.join(os.path.dirname(__file__), "test.txt")
        with open(local_file, "r", encoding="utf8") as localfile:

            # Upload file to the mocked bukcet
            s3_client = S3()
            s3_client.upload_file(local_file, BUCKET_NAME, "test.txt")

            # Delete local file
            os.remove(local_file)

            # Download the previously uploaded file back into local
            downloaded_file = os.path.join(
                os.path.dirname(__file__), "test_downloaded.txt"
            )
            s3_client.download_file(
                bucket_name=BUCKET_NAME,
                object_name="test.txt",
                file_name=downloaded_file,
            )

            with open(downloaded_file, "r", encoding="utf8") as downloadedfile:
                localfile_content = localfile.read()
                downloadedfile_content = downloadedfile.read()
                assert downloadedfile_content == localfile_content

                # Finally, if assertion passed, rewrite the file into "test.txt"
                with open(local_file, "w", encoding="utf8") as f:
                    f.write(downloadedfile_content)
                    os.remove(downloaded_file)  # delete the downloaded file
