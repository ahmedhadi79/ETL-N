import sys
import os
import unittest
from unittest.mock import patch, mock_open, MagicMock
import pandas as pd

sys.path.append(os.path.abspath("../"))
from lambda_function import read_sql_from_athena, write_to_s3, lambda_handler

class TestLambdaFunctions(unittest.TestCase):

    @patch("lambda_function.wr.athena.read_sql_query")
    @patch("lambda_function.open", new_callable=mock_open, read_data="SELECT * FROM some_table;")
    @patch("lambda_function.logger")
    def test_read_sql_from_athena_success(self, mock_logger, mock_open, mock_read_sql_query):
        mock_read_sql_query.return_value = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        success, df = read_sql_from_athena("data_path.sql", "data_database")
        
        self.assertTrue(success)
        self.assertIsInstance(df, pd.DataFrame)
        mock_open.assert_called_once_with("data_path.sql", "r")
        mock_read_sql_query.assert_called_once()
        mock_logger.info.assert_called_with("Reading from Athena... ")

    @patch("lambda_function.wr.athena.read_sql_query")
    @patch("lambda_function.open", new_callable=mock_open, read_data="SELECT * FROM some_table;")
    @patch("lambda_function.logger")
    def test_read_sql_from_athena_failure(self, mock_logger, mock_open, mock_read_sql_query):
        mock_read_sql_query.side_effect = Exception("Some error")
        success, df = read_sql_from_athena("data_path.sql", "data_database")
        
        self.assertFalse(success)
        self.assertIsNone(df)
        mock_logger.error.assert_any_call("Failed reading from Athena")

    @patch("lambda_function.data_catalog.schemas", {"data_table": {"col1": "int", "col2": "int"}})
    @patch("lambda_function.data_catalog.column_comments", {"data_table": {"col1": "Column 1", "col2": "Column 2"}})
    @patch("lambda_function.wr.s3.to_parquet")
    @patch("lambda_function.logger")
    def test_write_to_s3_success_with_partition(self, mock_logger, mock_to_parquet):
        mock_to_parquet.return_value = {"paths": ["s3://data_path/"], "partitions_values": {"col1": [1, 2]}}
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        
        res = write_to_s3(df, "data_table", "data_database", ["col1"], "data_bucket")
        self.assertIsNotNone(res)
        # mock_to_parquet.assert_called_once()
        mock_logger.info.assert_any_call("Uploading to S3 bucket: data_bucket")

    @patch("lambda_function.data_catalog.schemas", {"data_table": {"col1": "int", "col2": "int"}})
    @patch("lambda_function.data_catalog.column_comments", {"data_table": {"col1": "Column 1", "col2": "Column 2"}})
    @patch("lambda_function.wr.s3.to_parquet")
    @patch("lambda_function.logger")
    def test_write_to_s3_failure_with_partition(self, mock_logger, mock_to_parquet):
        mock_to_parquet.side_effect = Exception("Upload error")
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        
        res = write_to_s3(df, "data_table", "data_database", ["col1"], "data_bucket")
        self.assertIsInstance(res, Exception)
        mock_logger.error.assert_any_call("Failed uploading to S3 location:  s3://data_bucket/data_table/")

    @patch("lambda_function.data_catalog.schemas", {"data_table": {"col1": "int", "col2": "int"}})
    @patch("lambda_function.data_catalog.column_comments", {"data_table": {"col1": "Column 1", "col2": "Column 2"}})
    @patch("lambda_function.wr.s3.to_csv")
    @patch("lambda_function.logger")
    def test_write_to_s3_success_without_partition(self, mock_logger, mock_to_csv):
        mock_to_csv.return_value = {"paths": ["s3://data_path/"]}
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        
        res = write_to_s3(df, "data_table", "data_database", None, "data_bucket")
        self.assertIsNotNone(res)
        # mock_to_csv.assert_called_once()
        mock_logger.info.assert_any_call("Uploading to S3 bucket: data_bucket")

    @patch("lambda_function.config.config", {"customer_timeline_detail": {"sql_path": "data_path.sql"}})
    @patch("lambda_function.read_sql_from_athena")
    @patch("lambda_function.write_to_s3")
    @patch("lambda_function.logger")
    def test_lambda_handler_success(self, mock_logger, mock_write_to_s3, mock_read_sql_from_athena):
        mock_read_sql_from_athena.return_value = (True, pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]}))
        mock_write_to_s3.return_value = {"paths": ["s3://data_path/"]}
        
        event = {}
        context = {}
        lambda_handler(event, context)
        
        mock_read_sql_from_athena.assert_called_once_with("data_path.sql", "datalake_raw")
        mock_write_to_s3.assert_called_once()
        mock_logger.info.assert_any_call("Result: {'paths': ['s3://data_path/']}")


if __name__ == '__main__':
    unittest.main()
