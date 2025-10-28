import sys
import logging
import boto3
import asyncio
import pandas as pd
import urllib.parse
from datetime import datetime, date
from awsglue.utils import getResolvedOptions
from api_client import APIClient
from custom_functions import raw_write_to_s3
from data_catalog import schemas

# Initialize S3 client
s3_client = boto3.client("s3")


class CustomFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\x1b[38;20m",
        logging.INFO: "\x1b[32;20m",
        logging.WARNING: "\x1b[33;20m",
        logging.ERROR: "\x1b[31;20m",
        logging.CRITICAL: "\x1b[31;1m",
    }
    RESET = "\x1b[0m"
    FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

    def format(self, record):
        log_fmt = self.COLORS.get(record.levelno, self.RESET) + self.FORMAT + self.RESET
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def initialize_logger(module_name):
    """
    Initializes the logger with a custom formatter.
    """
    logger = logging.getLogger(module_name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(CustomFormatter())
        logger.addHandler(handler)

    logger.info("Logger initialized successfully!")
    return logger


logger = initialize_logger("clearbank-transactions-tos3raw")


def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager.
    """
    logger.info(f"Retrieving secret: {secret_name}")
    try:
        secrets_manager = boto3.client("secretsmanager")
        secret_value = secrets_manager.get_secret_value(SecretId=secret_name)
        return secret_value.get("SecretString")
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {e}", exc_info=True)
        return None


def construct_query_string(params):
    """
    Constructs a URL-encoded query string from provided parameters.
    """
    return urllib.parse.urlencode(params)


async def fetch_transactions(cb_client, account_id, page_number, page_size=1000):
    """
    Fetch a page of transactions asynchronously and return as a DataFrame.
    """
    logger.info(f"Fetching page {page_number} for account {account_id}")

    query_string = construct_query_string({"pageNumber": page_number, "pageSize": page_size})

    try:
        response = cb_client.get(
            endpoint=f"Accounts/{account_id}?{query_string}",
            clean=True,
            flatten=True,
            df=False,
        )

        if isinstance(response, dict):
            response = [response]

        if not isinstance(response, list):
            logger.error(f"Unexpected response format for page {page_number}: {type(response)} - {response}")
            return None

        df = pd.DataFrame([
            {key: value for key, value in record.items() if not key.startswith("halLinks")}
            for record in response
        ])

        expected_columns = list(schemas["cb_accounts"].keys())
        for col in expected_columns:
            if col not in df.columns:
                df[col] = ""

        df = df[expected_columns]
        logger.info(f"Fetched {len(df)} records from page {page_number}")

        return df

    except Exception as e:
        logger.error(f"Error fetching transactions for page {page_number}: {e}", exc_info=True)
        return None


async def fetch_all_transactions(cb_client, account_id, total_pages):
    """
    Fetch all pages of transactions concurrently.
    """
    tasks = [fetch_transactions(cb_client, account_id, page) for page in range(1, total_pages + 1)]
    results = await asyncio.gather(*tasks)

    return [df for df in results if df is not None]


def calculate_total_pages(cb_client, account_id, page_size=1000):
    """
    Determines the total number of pages for transaction data.
    """
    logger.info(f"Calculating total pages for account {account_id} with page size {page_size}")

    try:
        page_number = 1
        total_records = 0

        while True:
            query_string = construct_query_string({"pageNumber": page_number, "pageSize": page_size})
            response = cb_client.get(
                endpoint=f"Accounts/{account_id}?{query_string}",
                clean=True,
                flatten=False,
                df=False,
            )

            if isinstance(response, dict) and "account" in response:
                response = [response["account"]]

            if isinstance(response, list):
                current_page_records = len(response)
                total_records += current_page_records

                if current_page_records == 0:
                    logger.info("No records found in the first page. Exiting.")
                    return 0

                if current_page_records < page_size:
                    break  # Last page reached

                page_number += 1
            else:
                logger.error(f"Unexpected response format while calculating total pages: {response}")
                return 0

        total_pages = (total_records + page_size - 1) // page_size  # Ceiling division
        logger.info(f"Total records: {total_records}, Total pages: {total_pages}")
        return total_pages

    except Exception as e:
        logger.error(f"Error calculating total pages: {e}", exc_info=True)
        return 0


def upload_to_s3(df, table_name, env):
    """
    Uploads data to S3 in Parquet format.
    """
    if df.empty:
        logger.error("No data available for upload. DataFrame is empty.")
        return False

    try:
        df["date"] = date.today().strftime("%Y-%m-%d")
        df["timestamp_extracted"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        df["timestamp_extracted"] = pd.to_datetime(df["timestamp_extracted"])

        success = raw_write_to_s3(
            ingested_df=df,
            table_name=f"cb_{table_name}",
            schemas=schemas,
            env=env,
            file_type="parquet",
            mode="append",
        )
        logger.info("Raw write to S3 upload completed successfully.")
        return success

    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}", exc_info=True)
        return False


async def main():
    args = getResolvedOptions(
        sys.argv,
        ["ENV", "CB_AUTH_DETAILS", "CB_API_KEY", "CB_BASE_URL", "MAIN_ACCOUNT_ID", "CB_TABLE"],
    )

    env = args["ENV"]
    api_key = args["CB_API_KEY"]
    cb_table = args["CB_TABLE"]
    token = get_secret(api_key)

    if not token:
        logger.error("Failed to retrieve API token. Exiting.")
        return

    cb_client = APIClient(auth=f"Bearer {token}", base_url=args["CB_BASE_URL"])
    main_account_id = args["MAIN_ACCOUNT_ID"]
    page_size = 1000

    total_pages = calculate_total_pages(cb_client, main_account_id, page_size)
    if total_pages == 0:
        logger.info("No data to fetch. Exiting.")
        return

    all_transactions = await fetch_all_transactions(cb_client, main_account_id, total_pages)

    combined_df = pd.concat(all_transactions, ignore_index=True)
    combined_df.drop(columns=["date", "timestamp_extracted"], errors="ignore", inplace=True)

    upload_to_s3(combined_df, cb_table, env)


if __name__ == "__main__":
    asyncio.run(main())
