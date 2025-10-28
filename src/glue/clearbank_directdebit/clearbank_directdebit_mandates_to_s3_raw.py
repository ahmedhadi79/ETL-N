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
        raise RuntimeError(f"Failed to retrieve secret: {secret_name}")

def construct_query_string(params):
    """
    Constructs a URL-encoded query string from the provided parameters.
    """
    return urllib.parse.urlencode(params)

async def fetch_mandates(cb_client, main_account_id, page_number, page_size=1000, virtualAccountIds=None, max_retries=5):
    """
    Fetch a page of mandates asynchronously for each virtual account, with retry logic for rate limiting.
    """
    logger.info(f"Fetching mandates for page {page_number} for account {main_account_id}")

    query_string = construct_query_string({
        "pageNumber": page_number,
        "pageSize": page_size
    })

    mandates_list = []
    try:
        for virtualAccountId in virtualAccountIds:
            retries = 0
            while retries < max_retries:
                try:
                    df = cb_client.get(
                        endpoint=f"Accounts/{main_account_id}/Virtual/{virtualAccountId}/Mandates?{query_string}",
                        filter_objects=["directDebitMandates"],
                        clean=True,
                        flatten=False,
                        df=True,
                    )
                    if df is not None:
                        # Add virtualAccountId to each resulting DataFrame
                        df["virtualAccountId"] = virtualAccountId
                        mandates_list.append(df)
                    await asyncio.sleep(1)
                    break
                except Exception as e:
                    if "404" in str(e):
                        logger.warning(f"Resource not found for virtualAccountId {virtualAccountId}. Skipping.")
                        break  # Skip further attempts for 404 errors
                    elif "429" in str(e):  # If rate-limited, retry with backoff
                        retries += 1
                        wait_time = 2 ** retries  # Exponential backoff (2^retries)
                        logger.warning(f"Rate limit exceeded. Retrying in {wait_time} seconds... (Attempt {retries}/{max_retries})")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Unexpected error fetching mandates for virtualAccountId {virtualAccountId}: {e}")
                        break

        if not mandates_list:
            logger.warning(f"No mandates found for page {page_number}")
            return pd.DataFrame()  # Return an empty DataFrame instead of None
        return pd.concat(mandates_list, ignore_index=True)
    except Exception as e:
        logger.error(f"Error fetching mandates for page {page_number}: {e}")
        raise  # Reraise the exception to notify the caller

async def fetch_mandates_in_batches(cb_client, main_account_id, page_number, virtualAccountIds, batch_size=50, max_retries=5):
    """
    Fetch mandates in batches instead of one by one.
    """
    results = []
    for i in range(0, len(virtualAccountIds), batch_size):
        batch = virtualAccountIds[i:i+batch_size]
        batch_results = await fetch_mandates(cb_client, main_account_id, page_number, page_size=1000, virtualAccountIds=batch, max_retries=max_retries)
        if batch_results is not None:
            results.append(batch_results)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

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
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching transactions for page {page_number}: {e}")
        return pd.DataFrame()

async def fetch_all_transactions(cb_client, main_account_id, cb_table, cb_filter_object, total_pages):
    """
    Fetch all pages of transactions concurrently.
    """
    tasks = [
        fetch_transactions(cb_client, main_account_id, cb_table, cb_filter_object, page_number)
        for page_number in range(1, total_pages + 1)
    ]
    return await asyncio.gather(*tasks, return_exceptions=True)

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
    if df.empty:
        logger.warning(f"No data to upload for table {table_name}.")
        return False

    # Adding 'date' and 'timestamp_extracted' columns
    df["date"] = date.today().strftime("%Y-%m-%d")
    df["timestamp_extracted"] = datetime.utcnow().strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]  # Add current UTC timestamp
    df["timestamp_extracted"] = pd.to_datetime(df["timestamp_extracted"])  # Convert to datetime type

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
    try:
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
    except Exception as e:
        logger.error(f"Failed to parse input arguments: {e}")
        return

    env = args["ENV"]
    api_key = args["CB_API_KEY"]
    cb_table = args["CB_TABLE"]
    cb_filter_object = args["CB_FILTER_OBJECT"]

    try:
        token = get_secret(api_key)
    except RuntimeError as e:
        logger.error(str(e))
        return  # Exit if secret cannot be retrieved

    cb_client = APIClient(auth=f"Bearer {token}", base_url=args["CB_BASE_URL"])
    main_account_id = args["MAIN_ACCOUNT_ID"]
    page_size = 1000

    try:
        total_pages = get_total_pages(cb_client, main_account_id, page_size, cb_table, cb_filter_object)
    except Exception as e:
        logger.error(f"Error calculating total pages: {e}")
        return

    if total_pages == 0:
        logger.info("No data to fetch. Exiting.")
        return

    try:
        all_transactions = await fetch_all_transactions(cb_client, main_account_id, cb_table, cb_filter_object, total_pages)
        combined_df = pd.concat([df for df in all_transactions if isinstance(df, pd.DataFrame)], ignore_index=True)
        combined_df = combined_df.reset_index(drop=True)
    except Exception as e:
        logger.error(f"Error fetching transactions: {e}")
        return

    if 'id' not in combined_df.columns:
        logger.error("The 'id' column is not present in the combined DataFrame. Exiting.")
        return

    virtualAccountIds = combined_df['id'].tolist()
    logger.info(f"Found {len(virtualAccountIds)} virtual account IDs")

    try:
        all_mandates = await fetch_mandates_in_batches(cb_client, main_account_id, 1, virtualAccountIds)
        if all_mandates.empty:
            logger.warning("No mandates found. Exiting.")
            return

        columns_to_drop = ["date", "timestamp_extracted"]
        all_mandates = all_mandates.drop(columns=columns_to_drop, errors="ignore")
        
        if not upload_to_s3(all_mandates, "directdebit_mandates", env):
            logger.error("Failed to upload mandates to S3")
    except Exception as e:
        logger.error(f"Error processing mandates: {e}")
        return

if __name__ == "__main__":
    asyncio.run(main())
