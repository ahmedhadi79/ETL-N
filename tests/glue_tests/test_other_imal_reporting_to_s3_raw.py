# import unittest
# import pandas as pd
# from unittest.mock import patch, MagicMock
# import sys
# import os
# sys.path.append(os.path.abspath("../../"))
# from other.imal_reporting_to_s3_raw.imal_reporting_to_s3_raw import (
#     get_schema,
#     write_to_s3,
#     archive_objects_from_s3,
#     is_date_column,
# )

# class TestIMALReportingToS3Raw(unittest.TestCase):

#     def test_is_date_column_all_valid_dates(self):
#         series = pd.Series(['01/01/2020', '02/01/2020', '03/01/2020'])
#         self.assertTrue(is_date_column(series))

#     def test_is_date_column_mixed_date_and_datetime(self):
#         series = pd.Series(['01/01/2020', '2020-01-02 10:00:00'])
#         self.assertFalse(is_date_column(series))

#     def test_is_date_column_only_datetime(self):
#         series = pd.Series(['2020-01-01 10:00:00', '2020-01-02 20:00:00'])
#         self.assertFalse(is_date_column(series))

#     def test_is_date_column_non_date_strings(self):
#         series = pd.Series(['hello', 'world'])
#         self.assertFalse(is_date_column(series))

#     def test_is_date_column_empty_series(self):
#         series = pd.Series([], dtype="object")
#         self.assertTrue(is_date_column(series))

#     def test_is_date_column_dates_with_null_values(self):
#         series = pd.Series(['01/01/2020', None, '02/01/2020'], dtype="object")
#         self.assertTrue(is_date_column(series))

#     def test_is_date_column_datetimes_with_null_values(self):
#         series = pd.Series(['2020-01-01 10:00:00', None, '2020-01-02 20:00:00'], dtype="object")
#         self.assertFalse(is_date_column(series))

#     def test_is_date_column_non_string_data(self):
#         series = pd.Series([1, 2, 3])
#         self.assertFalse(is_date_column(series))

#     def test_get_schema_empty_dataframe(self):
#         df = pd.DataFrame()
#         schema, transformed_df = get_schema(df, "2024-01-01")
#         self.assertEqual(schema, {"date": "date", "timestamp_extracted": "timestamp"})
#         self.assertIn("date", transformed_df.columns)
#         self.assertIn("timestamp_extracted", transformed_df.columns)

#     def test_get_schema_boolean_columns(self):
#         df = pd.DataFrame({'A': [True, False]})
#         schema, _ = get_schema(df, "2024-01-01")
#         self.assertEqual(schema['a'], 'boolean')

#     def test_get_schema_integer_columns(self):
#         df = pd.DataFrame({'A': [1, 2, 3]})
#         schema, _ = get_schema(df, "2024-01-01")
#         self.assertEqual(schema['a'], 'int')

#     def test_get_schema_float_columns(self):
#         df = pd.DataFrame({'A': [1.1, 2.2, 3.3]})
#         schema, _ = get_schema(df, "2024-01-01")
#         self.assertEqual(schema['a'], 'double')

#     def test_get_schema_datetime_columns(self):
#         df = pd.DataFrame({'A': [pd.Timestamp('20240101'), pd.Timestamp('20240201')]})
#         schema, _ = get_schema(df, "2024-01-01")
#         self.assertEqual(schema['a'], 'timestamp')

#     def test_get_schema_date_strings_columns(self):
#         df = pd.DataFrame({'A': ['01/01/2024', '02/01/2024']})
#         df['A'] = df['A'].astype(object)
#         schema, _ = get_schema(df, "2024-01-01")
#         self.assertEqual(schema['a'], 'date')

#     def test_get_schema_string_columns(self):
#         df = pd.DataFrame({'A': ['some', 'random', 'strings']})
#         schema, _ = get_schema(df, "2020-01-01")
#         self.assertEqual(schema['a'], 'string')

#     # Tests for write_to_s3
#     @patch('other.imal_reporting_to_s3_raw.imal_reporting_to_s3_raw.wr.s3.to_parquet')
#     def test_write_to_s3_success(self, mock_to_parquet):
#         mock_to_parquet.return_value = True
#         result = write_to_s3(pd.DataFrame(), 'table', {}, ['partition'], 'bucket')
#         self.assertTrue(result)

#     @patch('other.imal_reporting_to_s3_raw.imal_reporting_to_s3_raw.wr.s3.to_parquet')
#     def test_write_to_s3_failure(self, mock_to_parquet):
#         mock_to_parquet.side_effect = Exception("S3 write error")
#         result = write_to_s3(pd.DataFrame(), 'table', {}, [], 'bucket')
#         self.assertFalse(result)

#     # Tests for archive_objects_from_s3
#     @patch('other.imal_reporting_to_s3_raw.imal_reporting_to_s3_raw.boto3.client')
#     def test_archive_objects_valid_path(self, mock_boto3):
#         mock_s3 = MagicMock()
#         mock_boto3.return_value = mock_s3
#         result = archive_objects_from_s3("s3://bucket/path/file.txt")
#         self.assertIsNotNone(result)

#     @patch('other.imal_reporting_to_s3_raw.imal_reporting_to_s3_raw.boto3.client')
#     def test_archive_objects_invalid_path(self, mock_boto3):
#         mock_s3 = MagicMock()
#         mock_boto3.return_value = mock_s3
#         with self.assertRaises(Exception):
#             archive_objects_from_s3("hdfs://schema/table_name")

#     @patch('other.imal_reporting_to_s3_raw.imal_reporting_to_s3_raw.boto3.client')
#     def test_archive_objects_missing_path(self, mock_boto3):
#         mock_s3 = MagicMock()
#         mock_boto3.return_value = mock_s3
#         with self.assertRaises(Exception):
#             archive_objects_from_s3("")

#     @patch('other.imal_reporting_to_s3_raw.imal_reporting_to_s3_raw.boto3.client')
#     def test_archive_objects_s3_error(self, mock_boto3):
#         mock_s3 = MagicMock()
#         mock_boto3.return_value = mock_s3
#         mock_s3.copy.side_effect = Exception("S3 error")
#         result = archive_objects_from_s3("s3://bucket/path/file.txt")
#         self.assertEqual(result, "")