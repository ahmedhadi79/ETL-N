import sys
import os
sys.path.append(os.path.abspath("../"))
parent_directory = os.path.abspath(os.path.join(os.getcwd(), ".."))
import unittest
from unittest.mock import Mock, patch,MagicMock
from lambda_function import write_to_s3,read_sql_from_athena
import pandas as pd
from datetime import datetime

class TestReadSqlFromAthena(unittest.TestCase):
    @patch('lambda_function.wr.athena.read_sql_query')    
    def test_read_sql_from_athena_success(self, mock_read_sql_query):

        mock_read_sql_query.return_value = 'Mocked DataFrame'
        sql_path = os.path.join(parent_directory, 'customer_curated.sql')
        input_database = 'datalake_raw'
        result, df = read_sql_from_athena(sql_path, input_database)
        self.assertTrue(result)
        self.assertEqual(df, 'Mocked DataFrame')

    @patch("awswrangler.s3.to_csv")
    def test_write_to_s3(self, mock_to_csv):
        tempdf = Mock()
        athena_table = "income_wealth_DataLake"

        s3_bucket = "bb2-sandbox-datalake-curated"
        res, path = write_to_s3(tempdf, athena_table, s3_bucket)

        expected_path = f"s3://{s3_bucket}/{athena_table}/date ={str(datetime.now())[:10]}/"

        mock_to_csv.assert_called_with(
            df=tempdf,
            path=expected_path,
            dataset=True,
            mode="overwrite",
        )
        self.assertEqual((res, path), (mock_to_csv.return_value, expected_path))


if __name__ == '__main__':    
    unittest.main()
