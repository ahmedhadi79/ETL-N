import json
import awswrangler as wr
import pandas as pd
from datetime import datetime
from urllib.parse import unquote
import logging

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Define list of columns for amplitude
AMPLITUDE_COLUMNS = [
    "amplitude_event_type",
    "amplitude_id",
    "event_id",
    "event_time",
    "event_type",
    "server_received_time",
    "server_upload_time",
    "session_id",
    "source_id",
    "user_id",
    "device_id",
    "uuid",
    "platform",
    "device_type",
    "os_version",
    "country",
    "device_carrier",
    "city",
    "device_family",
    "dma",
    "language",
    "os_name",
    "start_version",
    "region",
    "user_properties",
    "event_properties",
]


def extract_data(s3_uri, file_format):
    logger.info(f"[extract_data]: Reading from {s3_uri}")

    readers = {
        "csv": wr.s3.read_csv,
        "parquet": wr.s3.read_parquet,
        "json": wr.s3.read_json,
        "json.gz": lambda path: wr.s3.read_json(
            path=path, compression="gzip", lines=True
        ),
    }

    reader = readers.get(file_format)
    if not reader:
        raise ValueError(f"Unsupported file format: {file_format}")

    return reader(path=s3_uri)


def apply_transformations(df, selected_columns, partition_date):
    logger.info("[apply_transformations]: Starting transformations")

    if selected_columns:
        df = df[list(set(df.columns) & set(selected_columns))].copy()

    if partition_date:
        df["year"] = str(partition_date.year).zfill(4)
        df["month"] = str(partition_date.month).zfill(2)
        df["day"] = str(partition_date.day).zfill(2)

    return df


def enrich_columns(df):
    logger.info("[enrich_columns]: Applying additional enrichments")

    if "dma" in df.columns:
        df["dma"] = pd.to_numeric(df["dma"], errors="coerce")

    if "os_version" in df.columns:
        df["os_version"] = df["os_version"].astype(str)

    if "event_properties" in df.columns:
        df["event_properties"] = df["event_properties"].apply(
            lambda x: json.loads(x)
            if pd.notnull(x) and isinstance(x, str)
            else ({} if x == {} else x)
        )
        for col in ["brandId", "type", "marketingId"]:
            df[col] = df["event_properties"].apply(
                lambda x: x.get(col) if pd.notnull(x) else ""
            )
            df[col] = df[col].apply(lambda v: "" if v is None else str(v))

    return df


def load_data(df, target_bucket, target_table, rows_chunk, partition_date):
    logger.info("[load_data]: Loading data to curated S3")

    df = enrich_columns(df)

    file_path = f"s3://{target_bucket}/{target_table}/"
    file_name = f"{target_table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    wr.s3.to_parquet(
        df=df,
        partition_cols=["year", "month", "day"] if partition_date else None,
        max_rows_by_file=rows_chunk,
        use_threads=True,
        mode="append",
        path=file_path,
        filename_prefix=file_name,
        index=False,
        dataset=True,
        database="datalake_curated",
        table=target_table,
        compression="snappy",
    )

    logger.info(f"[load_data]: Upload completed to {file_path}")


def process_etl(config):
    s3_uri = f"s3://{config['source_bucket']}/{config['object_key']}"
    partition_date = datetime.now() if not config["no_partition"] else None

    df = extract_data(s3_uri, config["file_format"])
    df = apply_transformations(df, config["list_columns"], partition_date)
    load_data(
        df,
        config["target_bucket"],
        config["target_table"],
        config["rows_chunk"],
        partition_date,
    )


def lambda_handler(event, context):
    logger.info(f"[lambda_handler]: Event received: {json.dumps(event)}")

    for record in event["Records"]:
        try:
            body = json.loads(record["body"])

            # Skip S3 TestEvent
            if "Event" in body and body.get("Event") == "s3:TestEvent":
                logger.info(
                    f"[lambda_handler]: Ignoring S3 TestEvent: {json.dumps(body)}"
                )
                continue

            # Check for real S3 event structure
            if "Records" not in body:
                logger.warning(
                    f"[lambda_handler]: Unexpected SQS body format: {json.dumps(body)}"
                )
                continue

            for s3_record in body["Records"]:
                if s3_record.get("eventSource") != "aws:s3":
                    logger.info(
                        f"[lambda_handler]: Not an S3 event: {json.dumps(s3_record)}"
                    )
                    continue

                bucket_name = s3_record["s3"]["bucket"]["name"]
                object_key = unquote(s3_record["s3"]["object"]["key"])

                # Skip non-json.gz files
                if not object_key.endswith(".json.gz"):
                    logger.info(
                        f"[lambda_handler]: Skipping file {object_key} as it's not .json.gz"
                    )
                    continue

                env = bucket_name.split("-")[-1]
                target_bucket = f"bb2-{env}-datalake-curated"

                config = {
                    "env": env,
                    "source_bucket": bucket_name,
                    "object_key": object_key,
                    "target_bucket": target_bucket,
                    "table_name": "mir_amplitude",
                    "target_table": "mir_amplitude",
                    "file_format": "json.gz",
                    "no_partition": False,
                    "partition_format": None,
                    "list_columns": AMPLITUDE_COLUMNS,
                    "rows_chunk": 400000,
                }

                logger.info(f"[lambda_handler]: Starting ETL for file {object_key}")
                process_etl(config)

        except Exception as e:
            logger.error(f"[lambda_handler]: Failed processing record: {e}")
            raise e
