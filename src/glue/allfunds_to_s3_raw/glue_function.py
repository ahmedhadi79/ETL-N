import ast
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import awswrangler as wr
import pandas as pd
from awsglue.utils import getResolvedOptions

from api_client import APIClient
from data_catalog import schemas

# Manual override for local/testing runs
_MANUAL_EVENT: Dict[str, Any] = {
    "athena_table_names": [],
    "portfolio_ids": [],
    "start_date": "",
    "end_date": "",
}

REQUIRED_ARGS = ["BASE_URL", "NOMO_ALLFUNDS_READ_ONLY", "S3_RAW", "ATHENA_TABLE_NAMES"]


def setup_logger(
    name: Optional[str] = None, level: int = logging.INFO
) -> logging.Logger:
    """Create (idempotent) logger.

    Ensures multiple calls don't add duplicate handlers.
    """
    logger = logging.getLogger(name or __name__)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        logger.propagate = False
    return logger


logger = setup_logger("allfunds_glue")


@dataclass
class ETLConfig:
    base_url: str
    auth: str
    s3_raw: str
    athena_table_names: List[str]


class AllFundsETL:
    """ETL orchestration for AllFunds -> S3 parquet (Glue/Athena friendly).

    Public API:
        run(): executes the full pipeline and returns a summary dictionary.
    """

    OPEN_POSITIONS_ENDPOINT = "/api/v1/analysis/positions/transactions"
    PERFORMANCE_ENDPOINT = "/api/v1/analysis/performance/transactions"

    def __init__(
        self,
        config: ETLConfig,
        client: APIClient,
        logger_: Optional[logging.Logger] = None,
    ):
        self.config = config
        self.client = client
        self.logger = logger_ or logger

    # ---- Input helpers ----
    @staticmethod
    def build_date_list(
        start_date: Optional[str], end_date: Optional[str]
    ) -> List[str]:
        """Return inclusive YYYYMMDD date list, or previous UTC day if no range supplied."""
        if start_date and end_date:
            try:
                start = datetime.strptime(start_date, "%Y%m%d")
                end = datetime.strptime(end_date, "%Y%m%d")
            except ValueError as exc:
                raise ValueError(
                    "start_date and end_date must be in YYYYMMDD format"
                ) from exc
            if start > end:
                raise ValueError("start_date must be <= end_date")
            return pd.date_range(start, end).strftime("%Y%m%d").tolist()

        prev = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
        return [prev]

    # ---- API helpers ----
    def fetch_portfolio_ids(self, limit: int = 1000) -> List[str]:
        """Fetch list of active portfolio ids from the AllFunds API."""
        self.logger.info("Fetching portfolio ids (limit=%d)", limit)
        try:
            data = self.client.get(
                endpoint="/api/v1/portfolios/contracts/search",
                query=f"limit={limit}&skip=0&status[0]=ENABLED&amountCurrency=USD",
                filter_objects=["data"],
            )
        except Exception:
            self.logger.exception("Failed to fetch portfolio ids from AllFunds API")
            raise

        if not data:
            self.logger.warning("No portfolio data returned from AllFunds API")
            return []

        ids = [str(obj.get("id")) for obj in data if obj.get("id")]
        self.logger.info("Retrieved %d portfolio ids", len(ids))
        return ids

    # ---- Normalization & persistence ----
    @staticmethod
    def normalize_record(obj: Dict[str, Any], portfolio_id: str) -> Dict[str, Any]:
        """Normalize a single API record to include required metadata and partition column 'date' (YYYYMMDD).

        Rules:
          - Preserve API original 'date' where possible as 'date_raw'.
          - Ensure 'date' contains YYYYMMDD string used for Athena partition.
        """
        rec = dict(obj)  # shallow copy
        rec["timestamp_extracted"] = datetime.now(timezone.utc)
        rec["portfolio_id"] = portfolio_id
        rec["date"] = rec["dateAsString"]
        rec["dateAsString"] = obj["date"]
        return rec

    def save_to_s3(self, df: pd.DataFrame, athena_table: str) -> None:
        """Write DataFrame to S3/Glue catalog as parquet with partitioning.

        Validates presence of 'date' partition column and non-empty frame.
        """
        s3_path = f"s3://{self.config.s3_raw}/{athena_table}/"
        if df.empty:
            self.logger.warning(
                "No data to save for table %s. Skipping S3 write.", athena_table
            )
            return

        if "date" not in df.columns:
            raise RuntimeError(
                "DataFrame must contain 'date' column for partitioning before saving."
            )

        self.logger.info("Saving %d rows to %s", len(df), s3_path)
        try:
            wr.s3.to_parquet(
                df=df,
                path=s3_path,
                database="datalake_raw",
                table=athena_table,
                partition_cols=["date"],
                mode="append",
                index=False,
                dataset=True,
                schema_evolution=True,
                compression="snappy",
                dtype=schemas.get(athena_table),
            )
            self.logger.info("Successfully saved to S3: %s", s3_path)
        except Exception:
            self.logger.exception("Failed to save DataFrame to S3 at %s", s3_path)
            raise

    # ---- Table-specific fetchers ----
    def fetch_open_positions(
        self, portfolio_id: str, date: str
    ) -> List[Dict[str, Any]]:
        body = {
            "date": date,
            "portfolioIds": [portfolio_id],
            "currency": "USD",
            "lang": "EN",
        }
        self.logger.debug("Request body for open positions: %s", body)
        records = (
            self.client.post(
                endpoint=self.OPEN_POSITIONS_ENDPOINT,
                json_body=body,
                filter_objects=["openPositions"],
                clean=True,
                flatten=True,
            )
            or []
        )
        return records

    def fetch_performance(self, portfolio_id: str, date: str) -> List[Dict[str, Any]]:
        body = {
            "dateFrom": date,
            "dateTo": date,
            "portfolioIds": [portfolio_id],
            "currency": "USD",
            "annualPerformance": False,
            "monthlyPerformance": False,
            "performanceContribution": False,
            "productsTwr": False,
            "rollingPerformance": False,
        }
        self.logger.debug("Request body for performance: %s", body)
        records = (
            self.client.post(
                endpoint=self.PERFORMANCE_ENDPOINT,
                json_body=body,
                filter_objects=["serie"],
                clean=True,
            )
            or []
        )
        return records

    # ---- Orchestration ----
    def process_table(
        self, athena_table: str, portfolio_ids: List[str], dates: List[str]
    ) -> Dict[str, Any]:
        """Process a single configured athena table: fetch, normalize and persist.

        Returns a dict with processed and failed details for the table.
        """
        if athena_table not in (
            "allfunds_transactions_open_positions",
            "allfunds_transactions_performance",
        ):
            raise ValueError(f"Provided athena table not configured: {athena_table}")

        all_records: List[Dict[str, Any]] = []
        processed: Dict[str, List[Dict[str, Any]]] = {}
        failed: Dict[str, List[Dict[str, Any]]] = {}

        for portfolio_id in portfolio_ids:
            for date in dates:
                try:
                    self.logger.info(
                        "Fetching %s for portfolio %s on date %s",
                        athena_table,
                        portfolio_id,
                        date,
                    )
                    if athena_table == "allfunds_transactions_open_positions":
                        records = self.fetch_open_positions(portfolio_id, date)
                    else:
                        records = self.fetch_performance(portfolio_id, date)

                    normalized = [
                        self.normalize_record(r, portfolio_id) for r in (records or [])
                    ]
                    all_records.extend(normalized)

                    processed.setdefault(portfolio_id, []).append(
                        {"date": date, "count": len(records)}
                    )
                    self.logger.info(
                        "Fetched %d records for portfolio %s on %s",
                        len(records),
                        portfolio_id,
                        date,
                    )
                except Exception as exc:
                    self.logger.exception(
                        "Error fetching for portfolio %s on date %s", portfolio_id, date
                    )
                    failed.setdefault(portfolio_id, []).append(
                        {"date": date, "error": str(exc)}
                    )
                    # continue processing other portfolios/dates

        df = pd.DataFrame(all_records)
        if not df.empty:
            df.reset_index(drop=True, inplace=True)

        self.save_to_s3(df, athena_table)

        return {"processed": processed, "failed": failed, "count": int(len(df))}

    def run(self, manual_event: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute ETL flow and return a summary."""
        manual_event = manual_event or {}
        table_overrides = manual_event.get("athena_table_names") or []
        portfolio_overrides = manual_event.get("portfolio_ids") or []

        tables = table_overrides or self.config.athena_table_names
        if not tables:
            raise ValueError("No athena tables specified to process")

        portfolio_ids = portfolio_overrides or self.fetch_portfolio_ids()
        if not portfolio_ids:
            self.logger.warning("No portfolios found; exiting early")
            return {"status": "no_data", "processed": {}, "failed": {}}

        dates = self.build_date_list(
            manual_event.get("start_date"), manual_event.get("end_date")
        )

        summary_processed: Dict[str, Any] = {}
        summary_failed: Dict[str, Any] = {}
        total_count = 0

        for table in tables:
            result = self.process_table(table, portfolio_ids, dates)
            summary_processed[table] = result.get("processed", {})
            summary_failed[table] = result.get("failed", {})
            total_count += result.get("count", 0)

        status = "completed" if not any(summary_failed.values()) else "partial"
        summary = {
            "status": status,
            "requested_dates": dates,
            "processed_records": total_count,
            "processed": summary_processed,
            "failed": summary_failed,
        }

        if status != "completed":
            raise RuntimeError(
                f"Partial failure during ETL: {json.dumps(summary, default=str, indent=2)}"
            )

        self.logger.info(
            "ETL completed successfully: %s", json.dumps(summary, default=str)
        )
        return summary


# ---- Glue entrypoint helpers ----


def load_runtime_args() -> Dict[str, str]:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    missing = [k for k in REQUIRED_ARGS if not args.get(k)]
    if missing:
        raise RuntimeError(f"Missing required args: {', '.join(missing)}")
    return args


def init_api_client(auth: str, base_url: str) -> APIClient:
    return APIClient(
        auth=auth,
        secrets_manager=True,
        base_url=base_url,
        login_url=f"{base_url}/api/v1/auth/token",
    )


def main() -> Dict[str, Any]:
    try:
        runtime = load_runtime_args()
        cfg = ETLConfig(
            base_url=runtime["BASE_URL"],
            auth=runtime["NOMO_ALLFUNDS_READ_ONLY"],
            s3_raw=runtime["S3_RAW"],
            athena_table_names=ast.literal_eval(str(runtime["ATHENA_TABLE_NAMES"])),
        )

        client = init_api_client(cfg.auth, cfg.base_url)
        etl = AllFundsETL(cfg, client)
        return etl.run(manual_event=_MANUAL_EVENT)
    except Exception:
        logger.exception("Function failed")
        raise


if __name__ == "__main__":
    result = main()
    logger.info(json.dumps(result, indent=2, default=str))
