import os
import boto3
import asyncio
import logging
import pandas as pd
import ast
import json
import awswrangler as wr
from typing import Literal
from datetime import datetime, timedelta, date
import traceback

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)

# Case1: Execution inside AWS Lambda
if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
    from data_catalog import schemas
    from api_client import APIClient


class CustomError(Exception):
    """Custom exception to handle errors more explicitly"""

    pass


def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager.
    Raises a CustomError in case of failure.
    """
    logger.info(f"Retrieving secret: {secret_name}")
    try:
        secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)
        return secret_value["SecretString"]
    except Exception as e:
        logger.error(
            f"Error retrieving secret {secret_name}: {e}\n{traceback.format_exc()}"
        )
        raise CustomError(f"Failed to fetch secret: {secret_name}")


def construct_query_string(params):
    """
    Constructs a query string manually without URL encoding.
    """
    return "&".join([f"{key}={value}" for key, value in params.items()])


async def fetch_transactions(
    cb_client,
    main_account_id,
    cb_table,
    cb_filter_object,
    page_number,
    start_datetime,
    end_datetime,
    page_size=1000,
):
    """
    Fetch a page of transactions asynchronously for a given day.
    """
    logger.info(
        f"Fetching page {page_number} for account {main_account_id} for date {start_datetime}"
    )
    query_string = construct_query_string(
        {
            "startDateTime": f"{start_datetime}T00:00:00.00",
            "endDateTime": f"{end_datetime}T23:59:59.59",
            "pageNumber": page_number,
            "pageSize": page_size,
        }
    )

    try:
        df = cb_client.get(
            endpoint=f"Accounts/{main_account_id}/{cb_table}?{query_string}",
            filter_objects=[cb_filter_object],
            clean=True,
            flatten=False,
            df=True,
        )
        return df
    except Exception as e:
        logger.error(
            f"Error fetching transactions for page {page_number}: {e}\n{traceback.format_exc()}"
        )
        raise CustomError(f"Failed to fetch transactions for page {page_number}: {e}")


async def fetch_all_transactions_for_day(
    cb_client,
    main_account_id,
    cb_table,
    cb_filter_object,
    start_datetime,
    end_datetime,
    page_size=1000,
):
    """
    Fetch all pages of transactions for a specific day.
    """
    logger.info(f"Fetching transactions from {start_datetime} to {end_datetime}")
    all_transactions = []
    page_number = 1

    while True:
        df = await fetch_transactions(
            cb_client,
            main_account_id,
            cb_table,
            cb_filter_object,
            page_number,
            start_datetime,
            end_datetime,
            page_size,
        )
        if df is None or df.empty:
            break
        all_transactions.append(df)
        page_number += 1

    return all_transactions


def safe_convert_to_dict(value):
    """
    Safely convert string representations of dictionaries to actual dictionaries.
    """
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return None
    return value


async def fetch_transactions_for_date_range(
    cb_client,
    main_account_id,
    cb_table,
    cb_filter_object,
    start_date,
    end_date,
    page_size=1000,
):
    """
    Fetch transactions for each day in the specified date range.
    """
    all_transactions = []
    daily_transactions = await fetch_all_transactions_for_day(
        cb_client,
        main_account_id,
        cb_table,
        cb_filter_object,
        start_date,
        end_date,
        page_size,
    )
    if daily_transactions:
        all_transactions.extend(daily_transactions)
    return all_transactions


def raw_write_to_s3(
    ingested_df: pd.DataFrame,
    table_name: str,
    env: Literal["sandbox", "alpha", "beta", "prod"],
    mode: Literal["append", "overwrite", "overwrite_partitions"],
    schemas: dict = None,
    rows_chunk: int = 400000,
    no_partition: bool = False,
    boto3_session: boto3.Session = None,
):

    target_bucket_name = f"bb2-{env}-datalake-raw"
    path = f"s3://{target_bucket_name}/{table_name}/"
    logger.info(
        f"[load_to_s3]: Uploading to S3 path s3://{target_bucket_name}/{table_name}/"
    )

    # Step 1: Align the schema with the DataFrame columns, ensuring exact matching
    schema_columns = schemas[table_name].keys()
    df_columns = ingested_df.columns

    # Find common columns between the schema and the DataFrame
    common_columns = [col for col in schema_columns if col in df_columns]

    # Only keep the columns that are in both the schema and the DataFrame
    ingested_df = ingested_df[common_columns]

    # Set the column names exactly as per the schema
    ingested_df.columns = common_columns

    # Step 2: Debugging - Check non-numeric values in 'amount_instructedAmount'
    if "amount_instructedAmount" in ingested_df.columns:
        non_numeric_values = ingested_df[
            ~ingested_df["amount_instructedAmount"]
            .apply(pd.to_numeric, errors="coerce")
            .notnull()
        ]
        if not non_numeric_values.empty:
            logger.warning(
                f"Non-numeric values in 'amount_instructedAmount': {non_numeric_values['amount_instructedAmount'].unique()}"
            )

    # Step 3: Iterate over schema and apply the correct conversion
    for col, expected_type in schemas[table_name].items():
        if col in ingested_df.columns:
            try:
                if expected_type in ["date", "datetime"] or col in [
                    "timestamp_extracted",
                    "timestamp",
                ]:
                    # Convert to datetime using pandas to_datetime function
                    ingested_df[col] = pd.to_datetime(ingested_df[col], errors="coerce")
                elif expected_type == "int":  # Handle integer columns
                    ingested_df[col] = pd.to_numeric(
                        ingested_df[col], errors="coerce"
                    ).astype("Int64")
                elif expected_type == "float":  # Handle float columns
                    logger.info(f"Converting column: {col} to type: {expected_type}")
                    ingested_df[col] = pd.to_numeric(
                        ingested_df[col], errors="coerce"
                    ).astype(float)
                elif expected_type == "string":  # Handle string columns
                    ingested_df[col] = ingested_df[col].astype(str)
                else:
                    ingested_df[col] = ingested_df[col].astype(expected_type)
            except ValueError as e:
                logger.error(f"Could not convert {col} to {expected_type}: {e}")
            except Exception as e:
                logger.error(f"Error processing column {col}: {e}")

    # Adding 'date' and 'timestamp_extracted' columns
    ingested_df["date"] = date.today().strftime("%Y-%m-%d")
    ingested_df["timestamp_extracted"] = datetime.utcnow().strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[
        :-3
    ]  # Add current UTC timestamp
    ingested_df["timestamp_extracted"] = pd.to_datetime(
        ingested_df["timestamp_extracted"]
    )  # Convert to datetime type
    logger.info(f"Dataframe shape: {ingested_df.shape}")

    wr.s3.to_parquet(
        df=ingested_df,
        path=path,
        database="datalake_raw",
        table=table_name,
        partition_cols=None if no_partition else ["date"],
        mode=mode,
        max_rows_by_file=rows_chunk,
        use_threads=True,
        dataset=True,
        schema_evolution=True,
        compression="snappy",
        dtype=schemas[table_name],
        boto3_session=boto3_session,
    )


def upload_to_s3(df, table_name, env):
    """
    Uploads the data to S3 using a custom raw_write_to_s3 function.
    Raises CustomError if upload fails.
    """
    try:
        raw_write_to_s3(
            ingested_df=df,
            table_name=f"cb_{table_name}",
            schemas=schemas,
            env=env,
            mode="append",
        )
        logger.info(f"Data successfully uploaded to S3 for table {table_name}")
    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}\n{traceback.format_exc()}")
        raise CustomError(f"Failed to upload data to S3: {e}")


async def main():
    try:
        env = os.getenv("ENV")
        api_key = os.getenv("CB_API_KEY")
        cb_table = os.getenv("CB_TABLE")
        cb_filter_object = os.getenv("CB_FILTER_OBJECT")

        if not cb_table:
            raise CustomError("CB_TABLE is not set. Exiting.")

        token = get_secret(api_key)
        cb_client = APIClient(auth=f"Bearer {token}", base_url=os.getenv("CB_BASE_URL"))
        main_account_id = os.getenv("MAIN_ACCOUNT_ID")
        page_size = 1000

        # Fetch yesterday's date
        start_date = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
        end_date = start_date  # For a single day range

        all_transactions = await fetch_transactions_for_date_range(
            cb_client,
            main_account_id,
            cb_table,
            cb_filter_object,
            start_date,
            end_date,
            page_size,
        )

        # Handle cb_filter_object if it exists
        if cb_filter_object and cb_table.lower() == cb_filter_object.lower():
            cb_table_lower = cb_table.lower()
        elif cb_filter_object:
            cb_table_lower = cb_table.lower() + "_" + cb_filter_object.lower()
        else:
            cb_table_lower = cb_table.lower()

        if all_transactions:
            combined_df = pd.concat(
                [df for df in all_transactions if df is not None], ignore_index=True
            )

            if not combined_df.empty:
                combined_df = combined_df.reset_index(drop=True)

                # List of dictionary-like columns
                dict_columns = [
                    "amount",
                    "counterpartAccount",
                    "ultimateRemitterAccount",
                    "ultimateBeneficiaryAccount",
                ]

                # Flatten dict columns
                for col in dict_columns:
                    if col in combined_df.columns:
                        combined_df[col] = combined_df[col].apply(safe_convert_to_dict)
                        if combined_df[col].notnull().any():
                            flattened_col = pd.json_normalize(
                                combined_df[col].dropna(), sep="_"
                            )
                            flattened_col.columns = [
                                f"{col}_{subcol}" for subcol in flattened_col.columns
                            ]
                            combined_df = combined_df.drop(columns=[col]).join(
                                flattened_col
                            )

                # Drop 'date' and 'timestamp_extracted' columns
                columns_to_drop = ["date", "timestamp_extracted"]
                combined_df = combined_df.drop(columns=columns_to_drop, errors="ignore")

                # Upload the processed DataFrame to S3
                upload_to_s3(combined_df, cb_table_lower, env)

    except CustomError as e:
        logger.error(f"Error in main process: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}\n{traceback.format_exc()}")
        raise


def lambda_handler(event, context):
    logger.info(f"Lambda invoked with event: {event}")
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Lambda function failed: {e}\n{traceback.format_exc()}")
        raise e
