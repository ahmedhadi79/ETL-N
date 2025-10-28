import sys
import logging
import boto3
import asyncio
import pandas as pd
import urllib.parse
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
    Retrieves a secret from AWS Secrets Manager
    """
    logger.info(f"Retrieving secret: {secret_name}")
    try:
        secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)
        return secret_value["SecretString"]
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {e}")
        return None

def construct_query_string(params):
    """
    Constructs a URL-encoded query string from the provided parameters.
    """
    return urllib.parse.urlencode(params)

async def fetch_transactions(cb_client, main_account_id, cb_table, cb_filter_object, page_number, page_size=1000):
    """
    Fetch a page of transactions asynchronously.
    """
    logger.info(f"Fetching page {page_number} for account {main_account_id}")

    query_string = construct_query_string({
        "pageNumber": page_number,
        "pageSize": page_size
    })

    try:
        df = cb_client.get(
            endpoint=f"Accounts/{main_account_id}/{cb_table}?{query_string}",
            filter_objects=[cb_filter_object],
            clean=True,
            flatten=True,
            df=True,
        )
        return df
    except Exception as e:
        logger.error(f"Error fetching transactions for page {page_number}: {e}")
        return None

async def fetch_all_transactions(cb_client, main_account_id, cb_table, cb_filter_object, total_pages):
    """
    Fetch all pages of transactions concurrently.
    """
    tasks = [
        fetch_transactions(cb_client, main_account_id, cb_table, cb_filter_object, page_number)
        for page_number in range(1, total_pages + 1)
    ]
    return await asyncio.gather(*tasks)

def get_total_pages(cb_client, main_account_id, page_size, cb_table, cb_filter_object):
    """
    Fetches the total number of pages based on the number of records in the response.
    """
    logger.info(f"Calculating total pages for account {main_account_id} with page size {page_size}")

    try:
        page_number = 1
        total_records = 0

        while True:
            query_string = construct_query_string({"pageNumber": page_number, "pageSize": page_size})
            response = cb_client.get(
                endpoint=f"Accounts/{main_account_id}/{cb_table}?{query_string}",
                filter_objects=[cb_filter_object],
                clean=True,
                flatten=False,
                df=False,
            )

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
        logger.error(f"Error fetching total pages: {e}")
        return 0

def upload_to_s3(df, table_name, env):
    """
    Uploads the data to S3 using a custom raw_load_to_s3 function.
    """
    try:
        raw_write_to_s3(
            ingested_df=df,
            table_name=f"cb_{table_name}",
            schemas=schemas,
            env=env,
            file_type="parquet",
            mode="append",
        )
        logger.info(f"Data successfully uploaded to S3 for table {table_name}")
        return True
    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}")
        return False

async def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "ENV",
            "CB_AUTH_DETAILS",
            "CB_API_KEY",
            "CB_BASE_URL",
            "MAIN_ACCOUNT_ID",
            "CB_TABLE",
            "CB_FILTER_OBJECT"
        ],
    )
    env = args["ENV"]
    api_key = args["CB_API_KEY"]
    cb_table = args["CB_TABLE"]
    cb_filter_object = args["CB_FILTER_OBJECT"]
    token = get_secret(api_key)
    if not token:
        logger.error("Failed to retrieve API token. Exiting.")
        return

    cb_client = APIClient(auth=f"Bearer {token}", base_url=args["CB_BASE_URL"])
    main_account_id = args["MAIN_ACCOUNT_ID"]
    page_size = 1000

    total_pages = get_total_pages(cb_client, main_account_id, page_size, cb_table, cb_filter_object)
    if total_pages == 0:
        logger.info("No data to fetch. Exiting.")
        return

    all_transactions = await fetch_all_transactions(cb_client, main_account_id, cb_table, cb_filter_object, total_pages)

    combined_df = pd.concat([df for df in all_transactions if df is not None], ignore_index=True)
    combined_df = combined_df.reset_index(drop=True)

    # Convert cb_filter_object to string in case it's not
    if cb_table.lower() == str(cb_filter_object).lower():
        cb_table_lower = cb_table.lower()
    else:
        cb_table_lower = cb_table.lower() + "_" + str(cb_filter_object).lower()

    if upload_to_s3(combined_df, cb_table_lower, env):
        logger.info("Data processing and upload completed successfully.")
    else:
        logger.error("Data processing failed.")

if __name__ == "__main__":
    asyncio.run(main())
