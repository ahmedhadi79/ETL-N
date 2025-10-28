import os
import json
import unittest
from unittest.mock import patch, MagicMock
from datetime import date

# Env needed by the lambda
os.environ.setdefault("S3_RAW", "bb2-test-raw")

import sys

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../src/lambdas/cards_paymentology_data_to_s3_raw",
        )
    ),
)

import lambda_function as lf  # noqa: E402


class TestCardsPaymentologyDataToS3Raw(unittest.TestCase):
    def setUp(self):
        # Patch the module-level S3 client created at import time
        self.patcher_s3 = patch.object(lf, "S3", autospec=True)
        self.mock_s3 = self.patcher_s3.start()

        # Common paginator mock (override per test as needed)
        self.mock_paginator = MagicMock()
        self.mock_s3.get_paginator.return_value = self.mock_paginator

    def tearDown(self):
        self.patcher_s3.stop()

    # ---------- Small helpers ----------

    def _client_error(self, code="404", op="HeadObject"):
        return lf.ClientError({"Error": {"Code": code, "Message": "x"}}, op)

    # ---------- Unit tests for small functions ----------

    def test_detect_group_from_filename(self):
        self.assertEqual(lf._detect_group_from_filename("AAA_Fees_BBB.csv"), "fees")
        self.assertEqual(
            lf._detect_group_from_filename("x_INTERCHANGE_y.csv"), "interchange"
        )
        self.assertEqual(
            lf._detect_group_from_filename("z_presentments_q.csv"), "presentments"
        )
        self.assertIsNone(lf._detect_group_from_filename("random.csv"))
        self.assertIsNone(lf._detect_group_from_filename("random.txt"))

    def test_extract_date_from_filename_single_and_range(self):
        # Single date
        single = lf._extract_date_from_filename("BB2_Fees_20221216.csv")
        self.assertIsNotNone(single)
        s, e = single
        self.assertEqual(s, date(2022, 12, 16))
        self.assertEqual(e, date(2022, 12, 16))

        # Range
        rng = lf._extract_date_from_filename(
            "BB2_Fees_20221216_to_20221218_REGEN_01.csv"
        )
        self.assertIsNotNone(rng)
        s2, e2 = rng
        self.assertEqual(s2, date(2022, 12, 16))
        self.assertEqual(e2, date(2022, 12, 18))

        # No date
        self.assertIsNone(lf._extract_date_from_filename("BB2_Fees_DEC_16_2022.csv"))

    # ---------- _handle_one_s3_event ----------

    def test_handle_one_s3_event_happy_path_copy(self):
        # head_object -> 404 => not exists => copy
        self.mock_s3.head_object.side_effect = self._client_error("404")

        src_bucket = "bb2-test-curated"
        # %20 must be decoded before copy
        encoded_key = "user_upload_data/BB2%20DIGITAL_Presentments_20221216.csv"
        ok, msg = lf._handle_one_s3_event(
            src_bucket=src_bucket, src_key_encoded=encoded_key
        )

        self.assertTrue(ok)
        self.assertIn(
            "Copied to s3://bb2-test-raw/paymentology_presentments_data/BB2 DIGITAL_Presentments_20221216.csv",
            msg,
        )

        # Assert copy called with decoded key
        self.mock_s3.copy_object.assert_called_once()
        _, kwargs = self.mock_s3.copy_object.call_args
        self.assertEqual(
            kwargs["CopySource"]["Key"],
            "user_upload_data/BB2 DIGITAL_Presentments_20221216.csv",
        )
        self.assertEqual(kwargs["Bucket"], "bb2-test-raw")
        self.assertEqual(
            kwargs["Key"],
            "paymentology_presentments_data/BB2 DIGITAL_Presentments_20221216.csv",
        )

    def test_handle_one_s3_event_idempotent_already_exists(self):
        # head_object OK => already exists
        self.mock_s3.head_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        ok, msg = lf._handle_one_s3_event(
            src_bucket="b",
            src_key_encoded="user_upload_data/AA_Interchange_20240101.csv",
        )
        self.assertTrue(ok)
        self.assertIn("Already exists:", msg)
        self.mock_s3.copy_object.assert_not_called()

    def test_handle_one_s3_event_no_group(self):
        # File is CSV but doesn't contain fees/interchange/presentments
        self.mock_s3.head_object.side_effect = self._client_error("404")
        ok, msg = lf._handle_one_s3_event(
            src_bucket="b",
            src_key_encoded="user_upload_data/random_20240101.csv",
        )
        self.assertTrue(ok)
        self.assertIn("no group match", msg)
        self.mock_s3.copy_object.assert_not_called()

    # ---------- _run_backfill (with paginator) ----------

    def test_run_backfill_filters_by_date_and_copies(self):
        # Two CSVs in prefix: one within range, one outside.
        self.mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "sandbox-paymentology/BB2_Fees_20221216.csv"},
                    {"Key": "sandbox-paymentology/BB2_Presentments_20200101.csv"},
                ]
            }
        ]

        # head for first => 404 => copy; second would be skipped by date range
        self.mock_s3.head_object.side_effect = [
            self._client_error("404"),  # for 2022 file
        ]

        res = lf._run_backfill(
            src_bucket="bb2-sandbox-datalake-curated",
            prefix="sandbox-paymentology/",
            start_date="2022-01-01",
            end_date="2022-12-31",
        )

        summary = res["summary"]
        self.assertEqual(summary["processed"], 1)  # only the in-range file processed
        self.assertEqual(summary["copied"], 1)
        self.assertEqual(summary["skipped"], 0)
        self.mock_s3.copy_object.assert_called_once()

    def test_run_backfill_skips_files_without_date_when_filters_given(self):
        self.mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "p/BB2_Fees_DEC_16_2022.csv"}]}
        ]

        res = lf._run_backfill(
            src_bucket="bkt",
            prefix="p/",
            start_date="2022-12-01",
            end_date="2022-12-31",
        )
        self.assertEqual(res["summary"]["processed"], 0)
        self.assertEqual(res["summary"]["copied"], 0)
        self.assertEqual(res["summary"]["skipped"], 1)
        self.mock_s3.copy_object.assert_not_called()

    # ---------- lambda_handler modes ----------

    def test_lambda_handler_backfill_direct_invoke(self):
        # Backfill event
        event = {
            "backfill": {
                "src_bucket": "bb2-sandbox-datalake-curated",
                "prefix": "sandbox-paymentology/",
                "start_date": "2022-12-01",
                "end_date": "2022-12-31",
            }
        }

        # One in-range key
        self.mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "sandbox-paymentology/BB2_Presentments_20221216.csv"},
                ]
            }
        ]
        # Not exists -> copy
        self.mock_s3.head_object.side_effect = self._client_error("404")

        resp = lf.lambda_handler(event, None)
        self.assertIsInstance(resp, dict)
        self.assertIn("summary", resp)
        self.assertEqual(resp["summary"]["copied"], 1)
        self.mock_s3.copy_object.assert_called_once()

    def test_lambda_handler_sqs_mode_minimal(self):
        # SQS-wrapped S3 event (normal path). The current lambda returns True, so we just ensure no crash & copy invoked.
        event = {
            "Records": [
                {
                    "messageId": "m-1",
                    "body": json.dumps(
                        {
                            "Records": [
                                {
                                    "eventSource": "aws:s3",
                                    "s3": {
                                        "bucket": {
                                            "name": "bb2-sandbox-datalake-curated"
                                        },
                                        "object": {
                                            "key": "sandbox-paymentology/BB2_Interchange_20240101.csv"
                                        },
                                    },
                                }
                            ]
                        }
                    ),
                }
            ]
        }

        self.mock_s3.head_object.side_effect = self._client_error("404")
        resp = lf.lambda_handler(event, None)
        self.assertTrue(resp)  # True is the lambda's return in SQS mode
        self.mock_s3.copy_object.assert_called_once()


if __name__ == "__main__":
    unittest.main()
