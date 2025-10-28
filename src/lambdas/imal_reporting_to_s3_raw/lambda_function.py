import logging
from datetime import datetime
import awswrangler as wr
import pandas as pd
import boto3
import json
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def is_date_column(series):
    for item in series.dropna().unique():
        try:
            parsed_date = pd.to_datetime(str(item))
            if parsed_date.time() != pd.Timestamp(0).time():
                return False
        except ValueError:
            return False
    return True


def get_schema(df, date_string):
    df = df.rename(columns=str.lower)
    dtype_mapping = {
        "int64": "int",
        "int32": "int",
        "float64": "double",
        "float32": "double",
        "bool": "boolean",
        "object": "string",
        "datetime64": "timestamp",
        "timedelta": "string",
        "category": "string",
    }
    athena_schema = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        if dtype == "object" and is_date_column(df[col]):
            athena_schema[col] = "date"
        elif dtype.startswith("datetime64") or dtype.startswith("timedelta"):
            dtype = dtype.split("[")[0]
            athena_schema[col] = dtype_mapping.get(dtype, "string")
        else:
            athena_schema[col] = dtype_mapping.get(dtype, "string")

    for col in df.columns:
        if athena_schema[col] in ("date", "timestamp"):
            df[col] = pd.to_datetime(df[col], dayfirst=True)

    df["date"] = date_string
    athena_schema["date"] = "date"

    df["timestamp_extracted"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    athena_schema["timestamp_extracted"] = "timestamp"

    return athena_schema, df


def write_to_s3(df, athena_table, athena_schema, partition_columns, s3_bucket):
    path = f"s3://{s3_bucket}/{athena_table}/"
    logger.info(f"Writing to {path} with shape {df.shape}")

    try:
        wr.s3.to_parquet(
            df=df,
            path=path,
            index=False,
            dataset=True,
            database="datalake_raw",
            table=athena_table,
            mode="append",
            compression="snappy",
            schema_evolution="true",
            partition_cols=partition_columns,
            dtype=athena_schema,
        )
        return True
    except Exception as e:
        logger.error(f"Write failed: {e}")
        return False


def process_chunk(df, date_string, athena_table, partition_columns, s3_bucket):
    schema, df = get_schema(df, date_string)
    return write_to_s3(df, athena_table, schema, partition_columns, s3_bucket)


def load_json_in_chunks(
    abs_path, athena_table, s3_bucket, date_string, chunk_size=1000
):
    s3_client = boto3.client("s3")
    bucket, key = abs_path.replace("s3://", "").split("/", 1)
    content = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()

    data = json.loads(content)
    chunk = []
    for item in data:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            df = pd.DataFrame(chunk)
            process_chunk(df, date_string, athena_table, ["date"], s3_bucket)
            chunk.clear()

    if chunk:
        df = pd.DataFrame(chunk)
        process_chunk(df, date_string, athena_table, ["date"], s3_bucket)

    logger.info(f"Done processing {abs_path}")


def lambda_handler(event, context):
    logger.info("Event received: %s", event)
    try:
        dest_bucket = os.environ["dest_bucket"]
        bucket_name = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"]

        if not bucket_name or not key:
            logger.error("Missing bucket or key in EventBridge detail")
            return {"statusCode": 400, "body": "Invalid event structure"}

        abs_path = f"s3://{bucket_name}/{key}"
        filename = key.split("/")[-1]
        base_name = filename.split("_")[0]
        date_string = filename.split("_")[1][:8]
        table_name = f"imal_reporting_{base_name.lower()}"

        load_json_in_chunks(
            abs_path=abs_path,
            athena_table=table_name,
            s3_bucket=dest_bucket,
            date_string=date_string,
        )

        return {"statusCode": 200, "body": f"Processed {key}"}

    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        return {"statusCode": 500, "body": "Internal server error"}
