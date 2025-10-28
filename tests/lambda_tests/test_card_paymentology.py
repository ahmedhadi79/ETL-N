import json
import types
import pandas as pd
import pytest

import src.lambdas.cards_paymentology_data_to_s3_raw.lambda_function as lambda_function

class InvalidArgumentValue:
    """A stand-in for any non-JSON-serializable object coming from a lib."""


@pytest.fixture(autouse=True)
def env_vars(monkeypatch):
    monkeypatch.setenv("SFTP_S3_BUCKET", "src-bucket")
    monkeypatch.setenv("SFTP_S3_KEY", "incoming/paymentology/")
    monkeypatch.setenv("S3_RAW", "raw-bucket")


@pytest.fixture
def mock_boto3(monkeypatch):
    # ---- boto3.client("s3") paginator -> one key that matches the Fees group
    class FakePaginator:
        def paginate(self, Bucket, Prefix):
            assert Bucket == "src-bucket"
            assert Prefix == "incoming/paymentology/"
            return [
                {
                    "Contents": [
                        {
                            "Key": "incoming/paymentology/BB2 DIGITAL AND TECHNOLOGY SERVICES LTD_Fees_20250101.csv"
                        }
                    ]
                }
            ]

    class FakeS3Client:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return FakePaginator()

    # ---- boto3.resource("s3") for processed log (no file on first read) and put on write
    class FakeS3Object:
        def __init__(self, bucket, key):
            self.bucket = bucket
            self.key = key

        def get(self):
            from botocore.exceptions import ClientError
            raise ClientError(
                error_response={"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
                operation_name="GetObject",
            )

        def put(self, Body):
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    class FakeS3Resource:
        def Object(self, bucket, key):
            return FakeS3Object(bucket, key)

    def fake_client(name):
        assert name == "s3"
        return FakeS3Client()

    def fake_resource(name):
        assert name == "s3"
        return FakeS3Resource()

    import boto3
    monkeypatch.setattr(boto3, "client", fake_client)
    monkeypatch.setattr(boto3, "resource", fake_resource)


@pytest.fixture
def mock_wr(monkeypatch):
    """Mock awswrangler.s3.read_csv and .to_csv."""
    fake_wr = types.SimpleNamespace()
    fake_s3 = types.SimpleNamespace()

    def fake_read_csv(path):
        return pd.DataFrame({"col": [1]})

    def fake_to_csv(**kwargs):
        return {
            "paths": ["s3://raw-bucket/cards_paymentology_fees/part-000.csv"],
            "metadata": InvalidArgumentValue(),
        }

    fake_s3.read_csv = fake_read_csv
    fake_s3.to_csv = fake_to_csv
    fake_wr.s3 = fake_s3

    monkeypatch.setattr(lambda_function, "wr", fake_wr, raising=False)


def _run_lambda_and_json_dump():
    # Call the handler from the module import
    result = lambda_function.lambda_handler(event={}, context=None)
    return json.dumps(result)


def test_without_json_safe_reproduces_marshal_error(mock_boto3, mock_wr, monkeypatch):
    """
    Disable the module's _json_safe to simulate pre-fix behavior.
    json.dumps should then fail with a TypeError.
    """
    monkeypatch.setattr(lambda_function, "_json_safe", lambda x: x, raising=False)

    with pytest.raises(TypeError) as excinfo:
        _run_lambda_and_json_dump()

    assert "is not JSON serializable" in str(excinfo.value)


def test_with_json_safe_now_serializes_ok(mock_boto3, mock_wr):
    """
    With the real _json_safe, the return value should be JSON-serializable
    even though awswrangler returned a non-serializable object.
    """
    dumped = _run_lambda_and_json_dump()
    payload = json.loads(dumped)

    assert payload["processed_count"] == 1
    assert payload["queued_count"] == 1
    assert isinstance(payload["results"], list)
    assert payload["results"][0]["ok"] is True
    assert isinstance(payload["results"][0]["result"]["metadata"], str)
