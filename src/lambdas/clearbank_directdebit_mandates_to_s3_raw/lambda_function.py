import os
import boto3
import asyncio
import logging
import pandas as pd
from io import BytesIO
import awswrangler as wr
from datetime import datetime, timedelta, date
import traceback
import urllib.parse
from botocore.exceptions import ClientError

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


class S3Utils:
    """Utility class for S3 operations."""

    def __init__(self, boto_client):
        self.client = boto_client

    def list_parquet_files(self, bucket, prefix):
        try:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [
                obj["Key"]
                for obj in response.get("Contents", [])
                if obj["Key"].endswith(".parquet")
            ]
        except ClientError as e:
            logger.error(f"S3 ClientError listing files in bucket {bucket}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing files in bucket {bucket}: {e}")
            return []

    def load_parquet_files(self, bucket, keys):
        dataframes = []
        for key in keys:
            try:
                response = self.client.get_object(Bucket=bucket, Key=key)
                data = response["Body"].read()
                df = pd.read_parquet(BytesIO(data))
                dataframes.append(df)
            except ClientError as e:
                logger.error(f"S3 ClientError loading file {key}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error loading file {key}: {e}")
        return (
            pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
        )


def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager
    """
    logger.info(f"Retrieving secret: {secret_name}")
    try:
        secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)
        return secret_value["SecretString"]
    except ClientError as e:
        logger.error(
            f"Error retrieving secret {secret_name}: {e.response['Error']['Message']}"
        )
        raise RuntimeError(f"Failed to retrieve secret: {secret_name}")
    except Exception as e:
        logger.error(f"Unexpected error retrieving secret: {e}")
        raise RuntimeError(f"Unexpected error retrieving secret: {secret_name}")


async def fetch_mandates(
    cb_client,
    main_account_id,
    page_number,
    page_size=1000,
    virtualAccountIds=None,
    max_retries=5,
):
    logger.info(
        f"Fetching mandates for page {page_number} for account {main_account_id}"
    )
    query_string = urllib.parse.urlencode(
        {"pageNumber": page_number, "pageSize": page_size}
    )
    mandates_list = []

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
                    df["virtualAccountId"] = virtualAccountId
                    mandates_list.append(df)
                await asyncio.sleep(1)
                break
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    logger.warning(
                        f"Resource not found for virtualAccountId {virtualAccountId}. Skipping."
                    )
                    break
                elif e.response["Error"]["Code"] == "429":
                    retries += 1
                    wait_time = 2**retries
                    logger.warning(
                        f"Rate limit exceeded. Retrying in {wait_time} seconds... (Attempt {retries}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Unexpected error fetching mandates for virtualAccountId {virtualAccountId}: {e}"
                    )
                    break
            except Exception as e:
                logger.error(
                    f"Unexpected error fetching mandates for virtualAccountId {virtualAccountId}: {e}"
                )
                break

    if mandates_list:
        return pd.concat(mandates_list, ignore_index=True)
    else:
        logger.warning(f"No mandates found for page {page_number}")
        return pd.DataFrame()


async def fetch_mandates_in_batches(
    cb_client,
    main_account_id,
    page_number,
    virtualAccountIds,
    batch_size=50,
    max_retries=5,
):
    results = []
    for i in range(0, len(virtualAccountIds), batch_size):
        batch = virtualAccountIds[i : i + batch_size]
        batch_results = await fetch_mandates(
            cb_client,
            main_account_id,
            page_number,
            virtualAccountIds=batch,
            max_retries=max_retries,
        )
        if not batch_results.empty:
            results.append(batch_results)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def upload_to_s3(df, table_name, env, schemas, boto_session):
    if df.empty:
        logger.warning(f"No data to upload for table {table_name}.")
        return False

    df["date"] = date.today().strftime("%Y-%m-%d")
    df["timestamp_extracted"] = pd.to_datetime(
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    )

    target_bucket_name = f"bb2-{env}-datalake-raw"
    path = f"s3://{target_bucket_name}/{table_name}/"
    logger.info(
        f"[load_to_s3]: Uploading to S3 path s3://{target_bucket_name}/{table_name}/"
    )
    if schemas[table_name]:
        df.columns = schemas[table_name].keys()

    logger.info("Dataframe shape:  %s", df.shape)

    try:
        wr.s3.to_parquet(
            df=df,
            path=path,
            database="datalake_raw",
            table=table_name,
            partition_cols=["date"],
            mode="append",
            max_rows_by_file=400000,
            use_threads=True,
            dataset=True,
            schema_evolution=True,
            compression="snappy",
            dtype=schemas.get(table_name),
            boto3_session=boto_session,
        )
        logger.info(f"Data successfully uploaded to S3 for table {table_name}")
        return True
    except ClientError as e:
        logger.error(f"S3 ClientError uploading data to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error uploading data to S3: {e}")
        return False


async def main():
    try:
        env = os.getenv("ENV")
        api_key = os.getenv("CB_API_KEY")
        try:
            token = get_secret(api_key)
        except RuntimeError as e:
            logger.error(str(e))
            return

        cb_client = APIClient(auth=f"Bearer {token}", base_url=os.getenv("CB_BASE_URL"))
        main_account_id = os.getenv("MAIN_ACCOUNT_ID")

        s3_utils = S3Utils(boto3.client("s3"))
        bucket_name = f"bb2-{env}-datalake-raw"

        (
            today_date,
            yesterday_date,
        ) = datetime.now().date(), datetime.now().date() - timedelta(days=1)
        today_files = s3_utils.list_parquet_files(
            bucket_name, f"cb_virtual_accounts/date={today_date}/"
        )
        yesterday_files = s3_utils.list_parquet_files(
            bucket_name, f"cb_virtual_accounts/date={yesterday_date}/"
        )

        today_df = s3_utils.load_parquet_files(bucket_name, today_files)
        yesterday_df = s3_utils.load_parquet_files(bucket_name, yesterday_files)

        today_ids = set(today_df["id"]) if "id" in today_df.columns else set()
        yesterday_ids = (
            set(yesterday_df["id"]) if "id" in yesterday_df.columns else set()
        )

        only_in_today = pd.DataFrame(
            {"id": list(today_ids - yesterday_ids), "difference_type": "Only in Today"}
        )
        only_in_yesterday = pd.DataFrame(
            {
                "id": list(yesterday_ids - today_ids),
                "difference_type": "Only in Yesterday",
            }
        )
        result_df = pd.concat([only_in_today, only_in_yesterday], ignore_index=True)
        virtualAccountIds = result_df["id"].tolist()
        logger.info(
            f"Found {len(virtualAccountIds)} virtual account IDs for processing."
        )

        all_mandates = await fetch_mandates_in_batches(
            cb_client,
            main_account_id,
            page_number=1,
            virtualAccountIds=virtualAccountIds,
        )
        if not all_mandates.empty:
            columns_to_drop = ["date", "timestamp_extracted"]
            all_mandates.drop(columns=columns_to_drop, errors="ignore", inplace=True)
            upload_to_s3(
                all_mandates, "cb_directdebit_mandates", env, schemas, boto3.Session()
            )
            logger.info("Data successfully uploaded to S3")
        else:
            logger.warning("No mandates found. Exiting.")
    except Exception as e:
        logger.error(f"Main function error: {e}")
        raise


def lambda_handler(event, context):
    logger.info(f"Lambda invoked with event: {event}")
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Lambda function failed: {e}\n{traceback.format_exc()}")
        raise
