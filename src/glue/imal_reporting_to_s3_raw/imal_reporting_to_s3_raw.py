import logging
import sys
from datetime import datetime, timedelta
from typing import Optional
import awswrangler as wr
import pandas as pd
import boto3
from awsglue.utils import getResolvedOptions
import ijson


def setup_logger(
    name: Optional[str] = None,
    level: int = logging.INFO,
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename: Optional[str] = None,
) -> logging.Logger:
    """
    Sets up a logger with the specified configuration.

    Parameters:
    - name (Optional[str]): Name of the logger. If None, the root logger is used.
    - level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    - format (str): Log message format.
    - filename (Optional[str]): If specified, logs will be written to this file. Otherwise, logs are written to stdout.

    Returns:
    - logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if filename:
        handler = logging.FileHandler(filename)
    else:
        handler = logging.StreamHandler(sys.stdout)

    handler.setLevel(level)
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)

    # To avoid duplicate handlers being added
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger


logger = setup_logger("imal-reporting-to-s3-raw")


def is_date_column(series):
    for item in series.dropna().unique():
        try:
            parsed_date = pd.to_datetime(str(item))
            if parsed_date.time() != pd.Timestamp(0).time():
                return False
        except ValueError:
            return False
    return True


def get_schema(df, date_string: str):
    """
    Accepts a Pandas Dataframe, casts each column to correct datatype, and produces
    the Athena schema for each column in a dict.
    :param df: The Pandas Dataframe
    :return: The athena schema and new pandas dataframe
    """
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
            mapped_dtype = dtype_mapping.get(dtype, "string")
            athena_schema[col] = mapped_dtype
    for col in df.columns:
        if athena_schema[col] in ("date", "timestamp"):
            df[col] = pd.to_datetime(df[col], dayfirst=True)
    logger.info("Finished setting up schema.")
    df["date"] = date_string
    athena_schema["date"] = "date"

    df["timestamp_extracted"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    athena_schema["timestamp_extracted"] = "timestamp"

    return athena_schema, df


def write_to_s3(
    tempdf, athena_table, athena_schema, partition_columns, s3_bucket, write_mode
):
    """
    AWS Data Wrangler writing to S3
    :param tempdf: Pandas DF to write to S3
    :param athena_table: Table to write to
    :param athena_schema: Athena Schema
    :param partition_columns: The columns to use for partitioning the data
    :return: result
    """

    logger.info("Uploading to S3 bucket:  %s", s3_bucket)
    logger.info("Pandas DF shape:  %s", tempdf.shape)
    path = "s3://" + s3_bucket + "/" + athena_table + "/"
    logger.info("Uploading to S3 location:  %s", path)

    try:
        res = wr.s3.to_parquet(
            df=tempdf,
            path=path,
            index=False,
            dataset=True,
            database="datalake_raw",
            table=athena_table,
            mode=write_mode,
            compression="snappy",
            schema_evolution="true",
            partition_cols=partition_columns,
            dtype=athena_schema,
        )
        return True
    except Exception as e:
        logger.error("Athena schema:  %s", athena_schema)
        logger.error("Failed uploading to S3 location:  %s", path)
        logger.error("Exception occurred:  %s", e)

        return False


def process_chunk(
    chunk_df, date_string, athena_table, partition_columns, s3_bucket, write_mode
):
    """
    Function to process each chunk and write to S3.
    """
    logger.info(f"Processing chunk with shape: {chunk_df.shape}")
    athena_schema, chunk_df = get_schema(chunk_df, date_string)
    write_to_s3(
        chunk_df, athena_table, athena_schema, partition_columns, s3_bucket, write_mode
    )


def load_json_in_chunks(
    abs_path,
    athena_table,
    partition_columns,
    s3_bucket,
    date_string,
    chunk_size,
):
    """
    Stream and process a large JSON file from S3 in chunks using ijson.
    :param abs_path: S3 path to JSON file
    :param athena_table: Target Athena Glue table name
    :param partition_columns: List of columns to partition the data
    :param s3_bucket: S3 bucket to write data
    :param date_string: String of the day related to the chunk being processed
    :param chunk_size: Number of records per chunk to process
    """
    s3_client = boto3.client("s3")

    # Initialize required local vars
    bucket_name, key_name = abs_path.replace("s3://", "").split("/", 1)
    write_mode = "append"

    logger.info(f"Starting to stream and process file: {abs_path}")

    try:
        # Get json meta data and pointer from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key_name)
        content = response["Body"]

        # Stream over the JSON items
        chunk = []
        parser = ijson.items(content, "item")

        for item in parser:
            # Loading item per item untill reaching the chuck size
            chunk.append(item)
            if len(chunk) >= chunk_size:
                # Convert the chunk to a DataFrame
                chunk_df = pd.DataFrame(chunk)
                process_chunk(
                    chunk_df,
                    date_string,
                    athena_table,
                    partition_columns,
                    s3_bucket,
                    write_mode,
                )
                # Clear the chunk after processing
                chunk.clear()
                # Change the write mode for the next chuncks
                write_mode = "append"

        # Process any remaining records in the last chunk
        if chunk:
            chunk_df = pd.DataFrame(chunk)
            process_chunk(
                chunk_df,
                date_string,
                athena_table,
                partition_columns,
                s3_bucket,
                write_mode,
            )

        logger.info(f"Finished processing file: {abs_path}")

    except Exception as e:
        logger.error(f"Error while streaming and processing JSON from {abs_path}: {e}")


def main():

    # Step1: Initialize global vars
    args = getResolvedOptions(
        sys.argv,
        ["S3_RAW", "bucket_name", "backfill", "start_date", "end_date", "valid_files"],
    )
    s3_raw = args["S3_RAW"]
    bucket_name = args["bucket_name"]
    backfill = args["backfill"].lower() == "true"
    valid_files = args["valid_files"].split(",")
    objects_key = []
    if backfill:
        start_date = args["start_date"]
        end_date = args["end_date"]
        logger.info(
            f"Backfill mode is ON. Start date:{start_date}, End date:{end_date}"
        )
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        start_date = today
        end_date = today
        logger.info(f"Daily mode is ON. Start date:{today_str}, End date:{today_str}")

    # Step2: Listing target json files to load
    for file_prefix in valid_files:
        cur = start_date
        prefix = f"imal_reporting/{file_prefix}_"
        s3_client = boto3.client("s3")
        while cur <= end_date:
            res = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix + cur.strftime("%Y%m%d")
            )
            if "Contents" in res:
                for item in res["Contents"]:
                    key = item["Key"]
                    if (
                        key.endswith(".json")
                        and f'{prefix}{cur.strftime("%Y%m%d")}' in key
                    ):
                        objects_key.append(key)
            cur += timedelta(days=1)

    # Step3: Looping over each json file to load to s3_raw
    logger.info(f"Loading backlog:{objects_key}")
    for object_key in objects_key:
        logger.info(f"Start loading:{object_key}")
        filename = object_key.split("/")[1]
        abs_path = f"s3://{bucket_name}/{object_key}"

        match = filename.split("_")[0]
        logger.info(f"filename: {filename}")
        date_string = filename.split("_")[1][:8]
        logger.info(f"s3-path: {abs_path}")

        # Define target table and partition columns
        target_athena_glue_table = f"imal_reporting_{match.lower()}"
        partition_columns = ["date"]

        load_json_in_chunks(
            abs_path,
            target_athena_glue_table,
            partition_columns,
            s3_raw,
            date_string,
            chunk_size=1000000,
        )

        logger.info("Finished writing to: %s", target_athena_glue_table)

    logger.info(f"[Success]: finishied loading: {objects_key}")


if __name__ == "__main__":
    main()
