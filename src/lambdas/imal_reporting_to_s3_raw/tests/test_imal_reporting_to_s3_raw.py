import os
import sys


import unittest
from unittest.mock import patch, MagicMock

import pandas as pd
import awswrangler as wr
import base64
import json
from datetime import datetime
from datetime import date
from datetime import timezone

sys.path.append(os.path.abspath("../"))
from lambda_function import (
    is_date_column,
    get_schema,
    write_to_s3,
    process_chunk,
    load_json_in_chunks,
    lambda_handler
)


def test_is_date_column_true():
    series = pd.Series(["2024-04-01", "2024-04-02"])
    assert is_date_column(series) is True


def test_get_schema_structure():
    df = pd.DataFrame({
        "id": [1, 2],
        "created_at": ["2024-04-01", "2024-04-02"]
    })
    schema, new_df = get_schema(df, "20240401")
    assert schema["id"] == "int"
    assert schema["created_at"] == "date"
    assert "date" in schema
    assert "timestamp_extracted" in schema


@patch("lambda_function.wr.s3.to_parquet")
def test_write_to_s3_success(mock_to_parquet):
    mock_to_parquet.return_value = True
    df = pd.DataFrame([{"id": 1}])
    result = write_to_s3(
        df=df,
        athena_table="test_table",
        athena_schema={"id": "int"},
        partition_columns=["date"],
        s3_bucket="dest-bucket"
    )
    assert result is True
    mock_to_parquet.assert_called_once()


@patch("lambda_function.get_schema")
@patch("lambda_function.write_to_s3")
def test_process_chunk(mock_write_to_s3, mock_get_schema):
    mock_df = pd.DataFrame([{"id": 1}])
    mock_get_schema.return_value = ({"id": "int"}, mock_df)
    mock_write_to_s3.return_value = True

    result = process_chunk(mock_df, "20240401", "test_table", ["date"], "dest-bucket")
    assert result is True


@patch("lambda_function.boto3.client")
@patch("lambda_function.process_chunk")
def test_load_json_in_chunks(mock_process_chunk, mock_boto3):
    sample_data = [
        {"id": 1, "amount": 100},
        {"id": 2, "amount": 200}
    ]
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=json.dumps(sample_data).encode()))
    }

    load_json_in_chunks(
        abs_path="s3://source-bucket/key.json",
        athena_table="test_table",
        s3_bucket="dest-bucket",
        date_string="20240401",
        chunk_size=1
    )

    assert mock_process_chunk.call_count == 2


@patch.dict(os.environ, {"dest_bucket": "dest-bucket"})
@patch("lambda_function.load_json_in_chunks")
def test_lambda_handler_success(mock_loader):
    event = {
        "detail": {
            "bucket": {"name": "source-bucket"},
            "object": {"key": "transactions_20240401.json"}
        }
    }
    mock_loader.return_value = None
    result = lambda_handler(event, None)
    assert result["statusCode"] == 200
    assert "Processed" in result["body"]


@patch.dict(os.environ, {"dest_bucket": "dest-bucket"})
def test_lambda_handler_missing_key():
    event = {"detail": {"bucket": {}, "object": {}}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 500


def test_lambda_handler_unhandled_exception():
    event = {
        "detail": {
            "bucket": {"name": "my-bucket"},
            "object": {"key": "report_20250401.json"}
        }
    }

    context = {}

    with patch("lambda_function.load_json_in_chunks", side_effect=Exception("Something went wrong")):
        response = lambda_handler(event, context)
        assert response["statusCode"] == 500
        assert response["body"] == "Internal server error"