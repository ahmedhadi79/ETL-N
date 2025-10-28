import os
import sys
import pandas as pd
import json
from datetime import datetime
from unittest.mock import patch

sys.path.append(os.path.abspath("../"))
from lambda_function import (
    apply_transformations,
    enrich_columns,
    extract_data,
    load_data,
    lambda_handler,
)


def test_apply_transformations():
    data = {
        "event_type": ["click"],
        "user_id": [1],
        "extra": ["ignore"]
    }
    df = pd.DataFrame(data)

    selected_columns = ["event_type", "user_id"]
    partition_date = datetime(2024, 5, 2)

    result = apply_transformations(df, selected_columns, partition_date)

    assert set(result.columns) == {"event_type", "user_id", "year", "month", "day"}
    assert result["year"].iloc[0] == "2024"
    assert result["month"].iloc[0] == "05"
    assert result["day"].iloc[0] == "02"


def test_enrich_columns():
    df = pd.DataFrame({
        "dma": ["101", "invalid"],
        "os_version": [10.2, None],
        "event_properties": [
            '{"brandId": "abc", "type": "premium", "marketingId": "123"}',
            '{}'
        ]
    })

    enriched_df = enrich_columns(df)

    assert pd.api.types.is_numeric_dtype(enriched_df["dma"])
    assert enriched_df["os_version"].iloc[0] == "10.2"
    assert enriched_df["brandId"].tolist() == ["abc", ""]
    assert enriched_df["type"].tolist() == ["premium", ""]
    assert enriched_df["marketingId"].tolist() == ["123", ""]


@patch("awswrangler.s3.read_json")
def test_extract_data_json(mock_read_json):
    mock_df = pd.DataFrame({"event_type": ["click"]})
    mock_read_json.return_value = mock_df

    result = extract_data("s3://bucket/file.json", "json")

    mock_read_json.assert_called_once_with(path="s3://bucket/file.json")
    pd.testing.assert_frame_equal(result, mock_df)


@patch("awswrangler.s3.to_parquet")
def test_load_data(mock_to_parquet):
    df = pd.DataFrame({
        "dma": [101],
        "os_version": ["10"],
        "event_properties": ['{"brandId":"x"}'],
    })
    partition_date = datetime(2023, 12, 1)

    load_data(df, "my-bucket", "my-table", 1000, partition_date)

    assert mock_to_parquet.called
    args, kwargs = mock_to_parquet.call_args
    assert kwargs["partition_cols"] == ["year", "month", "day"]
    assert kwargs["dataset"] is True


def test_lambda_handler_triggers_etl():
    event = {
        "Records": [
            {
                "body": json.dumps({
                    "Records": [
                        {
                            "eventSource": "aws:s3",
                            "s3": {
                                "bucket": {"name": "bb2-dev-datalake-raw"},
                                "object": {"key": "test.json.gz"}
                            }
                        }
                    ]
                })
            }
        ]
    }

    with patch("lambda_function.process_etl") as mock_process_etl:
        lambda_handler(event, None)
        mock_process_etl.assert_called_once()
