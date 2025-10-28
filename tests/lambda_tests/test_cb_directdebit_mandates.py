import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
import pandas as pd
from io import BytesIO

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../src/lambdas/clearbank_directdebit_mandates_to_s3_raw")
    )
)
from lambda_function import (
    get_secret,
    S3Utils,
    upload_to_s3,
)

class TestS3Utils(unittest.TestCase):
    def setUp(self):
        # Mock S3 client
        self.mock_s3_client = MagicMock()
        self.s3_utils = S3Utils(boto_client=self.mock_s3_client)

    def test_list_parquet_files_success(self):
        # Mock response from S3 for listing objects
        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "path/to/file1.parquet"},
                {"Key": "path/to/file2.parquet"},
                {"Key": "path/to/file3.txt"},  # Non-parquet file
            ]
        }
        result = self.s3_utils.list_parquet_files("test-bucket", "prefix/")
        self.assertEqual(result, ["path/to/file1.parquet", "path/to/file2.parquet"])

    def test_list_parquet_files_client_error(self):
        self.mock_s3_client.list_objects_v2.side_effect = ClientError(
            error_response={"Error": {"Code": "403", "Message": "Forbidden"}},
            operation_name="ListObjectsV2"
        )
        result = self.s3_utils.list_parquet_files("test-bucket", "prefix/")
        self.assertEqual(result, [])  # Expect empty list on error

    def test_load_parquet_files_success(self):
        # Mock get_object to return a parquet file as bytes
        df = pd.DataFrame({"column": [1, 2, 3]})
        parquet_data = BytesIO()
        df.to_parquet(parquet_data)
        parquet_data.seek(0)

        self.mock_s3_client.get_object.return_value = {"Body": BytesIO(parquet_data.read())}
        result = self.s3_utils.load_parquet_files("test-bucket", ["file1.parquet"])

        pd.testing.assert_frame_equal(result, df)

    def test_load_parquet_files_client_error(self):
        self.mock_s3_client.get_object.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
            operation_name="GetObject"
        )
        result = self.s3_utils.load_parquet_files("test-bucket", ["file1.parquet"])
        self.assertTrue(result.empty)  # Expect empty DataFrame on error


class TestSecretManager(unittest.TestCase):
    @patch("boto3.client")
    def test_get_secret_success(self, mock_boto_client):
        mock_secretsmanager = mock_boto_client.return_value
        mock_secretsmanager.get_secret_value.return_value = {"SecretString": "my_secret"}
        result = get_secret("my_secret_name")
        self.assertEqual(result, "my_secret")

    @patch("boto3.client")
    def test_get_secret_client_error(self, mock_boto_client):
        mock_secretsmanager = mock_boto_client.return_value
        mock_secretsmanager.get_secret_value.side_effect = ClientError(
            error_response={"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
            operation_name="GetSecretValue"
        )
        with self.assertRaises(RuntimeError):
            get_secret("my_secret_name")


class TestUploadToS3(unittest.TestCase):
    @patch("awswrangler.s3.to_parquet")
    @patch("boto3.Session")
    def test_upload_to_s3_success(self, mock_boto_session, mock_to_parquet):
        # Modify the DataFrame to align with the schema's expected structure
        df = pd.DataFrame({"data": [1, 2, 3], "date": ["2023-10-01"]*3, "timestamp_extracted": [pd.Timestamp("2023-10-01T12:00:00Z")]*3})

        # Modify schema to match DataFrame
        schemas = {"my_table": {"data": "int", "date": "string", "timestamp_extracted": "timestamp"}}

        # Run the upload function
        result = upload_to_s3(df, "my_table", "test", schemas, mock_boto_session)

        self.assertTrue(result)
        mock_to_parquet.assert_called_once()  # Check if upload was attempted


if __name__ == "__main__":
    unittest.main()
