from unittest.mock import patch, MagicMock
import os
import sys
from datetime import datetime, date
import pandas as pd
import unittest

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../src/lambdas/clearbank_transactions_to_s3_raw")
    )
)
from lambda_function import (
    get_secret,
    safe_convert_to_dict,
    raw_write_to_s3,
    construct_query_string,
)

class TestCBTransactionsExecution(unittest.TestCase):

    @patch('lambda_function.boto3.client')
    def test_get_secret(self, mock_boto3_client):
        # Mock the SecretsManager response
        mock_secrets_manager = MagicMock()
        mock_secrets_manager.get_secret_value.return_value = {
            'SecretString': 'my_secret_value'
        }
        mock_boto3_client.return_value = mock_secrets_manager

        secret = get_secret('my_secret')
        assert secret == 'my_secret_value'
        mock_boto3_client.assert_called_once_with('secretsmanager')
        mock_secrets_manager.get_secret_value.assert_called_once_with(SecretId='my_secret')

    def test_construct_query_string(self):
        params = {
            'startDateTime': '2024-10-15T00:00:00.00',
            'endDateTime': '2024-10-15T23:59:59.59',
            'pageNumber': 1,
            'pageSize': 1000
        }
        result = construct_query_string(params)
        assert result == 'startDateTime=2024-10-15T00:00:00.00&endDateTime=2024-10-15T23:59:59.59&pageNumber=1&pageSize=1000'

    def test_safe_convert_to_dict(self):
        assert safe_convert_to_dict("{'key': 'value'}") == {'key': 'value'}
        assert safe_convert_to_dict('{"key": "value"}') == {"key": "value"}
        assert safe_convert_to_dict('invalid string') is None

    @patch('lambda_function.wr.s3.to_parquet')
    def test_raw_write_to_s3(self, mock_to_parquet):
        mock_schemas = {
            'test_table': {
                'date': 'date',
                'amount_instructedAmount': 'float',
                'timestamp_extracted': 'datetime'
            }
        }
        df = pd.DataFrame({
            'date': [date.today()],
            'amount_instructedAmount': [123.45],
            'timestamp_extracted': [datetime.utcnow()]
        })

        # Call the function with a mock dataframe
        raw_write_to_s3(df, 'test_table', 'sandbox', 'append', mock_schemas)

        # Check if the mock `to_parquet` was called
        mock_to_parquet.assert_called_once()
        args, kwargs = mock_to_parquet.call_args
        assert 'path' in kwargs
        assert 'df' in kwargs
        assert kwargs['partition_cols'] == ['date']


if __name__ == "__main__":
    unittest.main()
