import sys
import os

sys.path.append(os.path.abspath("../"))
parent_directory = os.path.abspath(os.path.join(os.getcwd(), ".."))
import config
import unittest
from unittest.mock import Mock, patch
from lambda_function import write_to_s3, read_sql_from_athena


class TestReadSqlFromAthena(unittest.TestCase):
    @patch("lambda_function.wr.athena.read_sql_query")
    def test_read_sql_from_athena_success(self, mock_read_sql_query):

        mock_read_sql_query.return_value = "Mocked DataFrame"
        sql_path = os.path.join(parent_directory, "customer_riskscore_data.sql")
        input_database = "datalake_raw"
        result, df = read_sql_from_athena(sql_path, input_database)
        self.assertTrue(result)
        self.assertEqual(df, "Mocked DataFrame")

    @patch("lambda_function.wr.s3.to_parquet")
    def test_write_to_s3_with_partition(self, mock_to_parquet):

        output_df = Mock()
        athena_table = "dynamo_sls_riskscore"
        database_name = "datalake_raw"
        partition_cols = ["date"]
        s3_bucket = "bb2-sandbox-datalake-curated"
        write_mode = "overwrite_partitions"

        result = write_to_s3(
            output_df,
            athena_table,
            database_name,
            partition_cols,
            s3_bucket,
            write_mode,
        )

        mock_to_parquet.assert_called_once_with(
            df=output_df,
            path=f"s3://{s3_bucket}/{athena_table}/",
            index=False,
            dataset=True,
            database=database_name,
            table=athena_table,
            mode="overwrite_partitions",
            schema_evolution="true",
            compression="snappy",
            partition_cols=partition_cols,
            dtype=config.config["customer_risk_score_data_raw_to_curated"]["catalog"],
        )

        self.assertEqual(result, mock_to_parquet.return_value)


if __name__ == "__main__":
    unittest.main()
