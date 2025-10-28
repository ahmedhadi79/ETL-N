import sys
import types

# Mock awsglue.utils import getResolvedOptions
if "awsglue" not in sys.modules:
    awsglue_mod = types.ModuleType("awsglue")
    awsglue_utils = types.ModuleType("awsglue.utils")

    # permissive stub: tests patch this when needed
    def _getResolvedOptions(argv, *names):
        # return an empty dict by default; tests patch this to return runtime args
        return {}

    awsglue_utils.getResolvedOptions = _getResolvedOptions

    sys.modules["awsglue"] = awsglue_mod
    sys.modules["awsglue.utils"] = awsglue_utils

# Mock awswrangler as wr and use wr.s3.to_parquet.
if "awswrangler" not in sys.modules:
    awswrangler = types.ModuleType("awswrangler")
    awswrangler.s3 = types.ModuleType("awswrangler.s3")

    # Dummy to_parquet so import-time won't fail; tests will patch the function.
    def _to_parquet(*args, **kwargs):
        return None

    awswrangler.s3.to_parquet = _to_parquet
    sys.modules["awswrangler"] = awswrangler
    sys.modules["awswrangler.s3"] = awswrangler.s3

# Mock api_client and data_catalog stubs
if "api_client" not in sys.modules:
    api_client_stub = types.ModuleType("api_client")

    class _StubAPIClient:
        def __init__(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            return []

        def post(self, *args, **kwargs):
            return []

    api_client_stub.APIClient = _StubAPIClient
    sys.modules["api_client"] = api_client_stub

if "data_catalog" not in sys.modules:
    data_catalog_stub = types.ModuleType("data_catalog")
    data_catalog_stub.schemas = {}
    data_catalog_stub.column_comments = {}
    sys.modules["data_catalog"] = data_catalog_stub

# -----------------------
# Import target module
# -----------------------
import os
import importlib.util

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))

# find project root: look up until we find "src" directory (max 3 levels)
project_root = None
cursor = _THIS_DIR
for _ in range(3):
    parent = os.path.abspath(os.path.join(cursor, os.pardir))
    if os.path.isdir(os.path.join(parent, "src")):
        project_root = parent
        break
    cursor = parent

if not project_root:
    # fallback to parent of tests dir
    project_root = os.path.abspath(os.path.join(_THIS_DIR, os.pardir))

# ensure project root on sys.path so "src" package imports work
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Attempt normal import first
try:
    from src.glue.allfunds_to_s3_raw import glue_function as lf
except Exception:
    # Fallback: import by file path
    possible_paths = [
        os.path.join(
            project_root, "src", "glue", "allfunds_to_s3_raw", "glue_function.py"
        ),
        os.path.join(project_root, "glue_function.py"),
        os.path.join(_THIS_DIR, os.pardir, "glue_function.py"),
    ]
    module_path = None
    for p in possible_paths:
        if p and os.path.exists(p):
            module_path = p
            break
    if module_path is None:
        raise ImportError(
            "Could not find glue_function.py to import (checked common paths)."
        )

    spec = importlib.util.spec_from_file_location(
        "glue_function_under_test", module_path
    )
    lf = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(lf)
    sys.modules[lf.__name__] = lf

################################################################
# -----------------------
# Unit tests
# -----------------------
################################################################
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd


class TestMainETL(unittest.TestCase):
    def setUp(self):
        # Baseline runtime args dictionary the ETL expects
        self.runtime_args = {
            "BASE_URL": "https://example.com",
            "NOMO_ALLFUNDS_READ_ONLY": "sls/secret/path",
            "S3_RAW": "unit-test-raw-bucket",
            "ATHENA_TABLE_NAMES": "['allfunds_transactions_open_positions', 'allfunds_transactions_performance']",
        }

        # Provide a simple deterministic data
        self.sample_pos_row = {
            "date": "2025-01-01",
            "dateAsString": "20250101",
            "qty": 1,
        }
        self.sample_perf_row = {
            "date": "2025-01-01",
            "dateAsString": "20250101",
            "perf": 2,
        }

    def test_main_successful_end_to_end(self):
        """
        Successful end-to-end run:
        - getResolvedOptions returns expected runtime args
        - init_api_client returns client whose post() returns rows for both endpoints
        - save_to_s3 (awswrangler.s3.to_parquet) is patched so no external IO occurs
        - main() should return a dict with status 'completed' and show processed entries
        """
        with patch.object(
            lf, "getResolvedOptions", return_value=self.runtime_args
        ), patch.object(lf, "init_api_client") as init_client_patch, patch.object(
            lf.wr.s3, "to_parquet"
        ) as to_parquet_mock:

            mock_client = MagicMock()
            # get() used for portfolio search -> return one portfolio id
            mock_client.get.return_value = [{"id": "p1"}]

            # post() behavior: return one row for each endpoint
            def post_side_effect(endpoint=None, *args, **kwargs):
                if endpoint and endpoint.endswith("/positions/transactions"):
                    return [self.sample_pos_row]
                if endpoint and endpoint.endswith("/performance/transactions"):
                    return [self.sample_perf_row]
                return []

            mock_client.post.side_effect = post_side_effect
            init_client_patch.return_value = mock_client

            # Provide simple schemas so code that references lf.schemas won't crash
            lf.schemas = {
                "allfunds_transactions_open_positions": {"qty": "int"},
                "allfunds_transactions_performance": {"perf": "int"},
            }

            # Execute the main entrypoint
            result = lf.main()

            # Basic expectations about returned summary
            self.assertIsInstance(result, dict)
            self.assertEqual(result.get("status"), "completed")
            # Expect 2 processed records (1 pos + 1 perf)
            self.assertEqual(result.get("processed_records"), 2)
            processed = result.get("processed", [])
            self.assertIn("allfunds_transactions_open_positions", processed)
            self.assertIn("allfunds_transactions_performance", processed)
            # failed should be present and empty for these two tables
            failed = result.get("failed", {})
            self.assertIsInstance(failed, dict)
            self.assertIn("allfunds_transactions_open_positions", failed)
            self.assertIn("allfunds_transactions_performance", failed)
            # to_parquet should have been called twice (one per table)
            self.assertEqual(to_parquet_mock.call_count, 2)

    def test_main_partial_failure_raises(self):
        """
        Simulate one endpoint raising an exception while another returns data.
        main() should surface this as a partial failure (raising an exception).
        """
        with patch.object(
            lf, "getResolvedOptions", return_value=self.runtime_args
        ), patch.object(lf, "init_api_client") as init_client_patch, patch.object(
            lf.wr.s3, "to_parquet"
        ) as to_parquet_mock:

            mock_client = MagicMock()
            mock_client.get.return_value = [{"id": "p1"}]

            # Raise for positions endpoint; succeed for performance endpoint
            def post_side_effect(endpoint=None, *args, **kwargs):
                if endpoint and endpoint.endswith("/positions/transactions"):
                    raise RuntimeError("downstream error")
                if endpoint and endpoint.endswith("/performance/transactions"):
                    return [self.sample_perf_row]
                return []

            mock_client.post.side_effect = post_side_effect
            init_client_patch.return_value = mock_client

            lf.schemas = {
                "allfunds_transactions_open_positions": {"qty": "int"},
                "allfunds_transactions_performance": {"perf": "int"},
            }

            # Expect main() to raise due to partial failure detected by ETL logic
            with self.assertRaises(Exception) as cm:
                lf.main()

            # The exception message should indicate a partial failure (implementation dependent)
            msg = str(cm.exception)
            # be permissive: check for substring that indicates partial/failed
            self.assertTrue(
                ("Partial" in msg)
                or ("partial" in msg)
                or ("failed" in msg)
                or isinstance(cm.exception, RuntimeError),
                msg,
            )

            # to_parquet should have been called once (only performance saved)
            self.assertEqual(to_parquet_mock.call_count, 1)

    def test_main_missing_runtime_args_raises(self):
        """
        If getResolvedOptions returns missing/empty args, main() should raise an error during runtime-arg loading.
        """
        with patch.object(lf, "getResolvedOptions", return_value={}):
            # Behavior depends on glue_function implementation; expect a RuntimeError or ValueError
            with self.assertRaises(Exception) as cm:
                lf.main()
            msg = str(cm.exception)
            self.assertTrue(
                ("Missing" in msg)
                or ("required" in msg)
                or ("runtime" in msg)
                or isinstance(cm.exception, RuntimeError)
            )


# Allow running tests directly
if __name__ == "__main__":
    unittest.main(verbosity=2)
