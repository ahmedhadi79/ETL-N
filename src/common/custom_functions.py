import ast
import io
import json
import logging
import os
import re
import sys
from datetime import date
from datetime import datetime
from datetime import timezone
from typing import Literal
from typing import Optional

import awswrangler as wr
import boto3
import pandas as pd
from flatten_json import flatten


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


# Deprecated function, start using setup_logger() instead.
def initialize_log(name) -> logging.Logger:
    """
    logging function with set level logging output
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


logger = initialize_log("common.functions")


def get_secret(secret_name, boto3_session=None):
    """
    Retrieves a secret from AWS Secrets Manager

    :param secret_name: The key to retrieve
    :return: The value of the secret
    """
    logger.info("Retrieving :  %s", secret_name)
    try:
        if boto3_session:
            secretsmanager = boto3_session.client("secretsmanager")
        else:
            secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value["SecretString"])
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        raise e


def select_schema(raw_df, schema, add_partition_flag=True):
    # select schema columns in dataframe
    selected_columns = [col for col in schema.keys() if col in raw_df.columns]
    logger.info("dataframe has the following columns: %s", selected_columns)
    transformed_df = raw_df[selected_columns]
    df = transformed_df.copy()
    for column in schema.keys():
        if column not in df.columns:
            logger.info("column not found in dataframe: %s", column)
            df[column] = None
        if schema.get(column) == "string":
            df[column] = df[column].astype(str)
        elif schema.get(column) == "timestamp":
            df[column] = pd.to_datetime(df[column], utc=True)
            df[column] = df[column].dt.tz_localize(None)
        elif schema.get(column) == "boolean":
            df[column] = df[column].astype(bool)
        elif schema.get(column) == "double":
            df[column] = df[column].astype(float)
        elif schema.get(column) == "int":
            df[column] = df[column].astype(int)
        else:
            df[column] = df[column].astype(str)

    column_list = list(schema.keys())
    df = df[column_list]

    if add_partition_flag:
        date_str = date.today().strftime("%Y%m%d")

        date_year = int(date_str[0:4])
        date_month = int(date_str[4:6])
        date_day = int(date_str[6:8])

        df["year"] = date_year
        df["month"] = date_month
        df["day"] = date_day

    return df


def fallback_write_to_s3(tempdf: pd.DataFrame, athena_table: str, s3_bucket: str):
    """
    Fallback Boto3 writing to S3.
    :param tempdf: Pandas DF to write to S3
    :type tempdf: pd.DataFrame
    :param athena_table: Table to write to (it will be suffixed with _fallback)
    :type athena_table: str
    :param s3_bucket: The S3 Bucket to write to
    :type s3_bucket: str
    """

    dt = datetime.utcnow()
    date = dt.strftime("%Y%m%d")
    time = dt.strftime("%H%M%S")

    csv_buffer = io.StringIO()
    tempdf.to_csv(csv_buffer)
    s3_resource = boto3.resource("s3")

    fallback_path = f"{athena_table}_fallback/{date}/{time}.csv"
    logger.info(
        f"Error occurred so uploading to S3 location: {fallback_path} as CSV..."
    )

    s3_resource.Object(s3_bucket, fallback_path).put(Body=csv_buffer.getvalue())


def raw_load_to_s3(
    ingested_df: pd.DataFrame,
    table_name: str,
    env: Literal["sandbox", "alpha", "beta", "prod"],
    file_type: Literal["csv", "json", "parquet"],
    mode: Literal["append", "overwrite", "overwrite_partitions"],
    column_comments: dict = None,
    schemas: dict = None,
    filtered_columns: list[str] = None,
    rows_chunk: int = 400000,
    no_partition: bool = False,
    boto3_session: boto3.Session = None,
):
    """
    Custom wrapper built over wrangler save functions
    to load a DataFrame to datalake S3 bucket in a specified file format.

    Parameters:
    - ingested_df (pd.DataFrame): The DataFrame to be loaded to S3.
    - table_name (str): The name of the table or object in the S3 bucket.
    - column_comments (dict): A dictionary containing column_comments.
    - schemas (dict): A dictionary containing schemas.
    - env (Literal["sandbox", "alpha", "beta", "prod"]): The environment in which the data is being loaded.
    - file_type (Literal["csv", "json", "parquet"]): The file format in which the data will be stored.
    - mode (Literal["append", "overwrite", "overwrite_partitions"]).
    - filtered_columns (list[str], optional): List of columns to include in the output. Default is None (all columns).
    - rows_chunk (int, optional): Number of rows to be written in each chunk. Default is 400,000.
    - no_partition (bool, optional): If True, data is stored without partitioning. Default is False.
    - boto3_session (boto3.Session, optional): A custom boto3 session. Default is None.
    """
    target_bucket_name = f"bb2-{env}-datalake-raw"
    path = f"s3://{target_bucket_name}/{table_name}/"
    logger.info(
        f"[load_to_s3]: Uploading to S3 path s3://{target_bucket_name}/{table_name}/"
    )

    if filtered_columns:
        ingested_df = ingested_df[
            [col for col in ingested_df.columns if col in filtered_columns]
        ]

    # Enforce pydantic columns only.
    if schemas[table_name]:
        ingested_df.columns = schemas[table_name].keys()

    logger.info("Dataframe shape:  %s", ingested_df.shape)

    try:
        if file_type == "parquet":
            wr.s3.to_parquet(
                df=ingested_df,
                path=path,
                database="datalake_raw",
                table=table_name,
                partition_cols=None if no_partition else ["date"],
                mode=mode,
                max_rows_by_file=rows_chunk,
                use_threads=True,
                index=False,
                dataset=True,
                schema_evolution=True,
                compression="snappy",
                dtype=schemas[table_name],
                glue_table_settings=wr.typing.GlueTableSettings(
                    columns_comments=column_comments[table_name]
                ),
                boto3_session=boto3_session,
            )
        elif file_type == "csv":
            wr.s3.to_csv(
                df=ingested_df,
                path=path,
                database="datalake_raw",
                table=table_name,
                partition_cols=None if no_partition else ["date"],
                mode=mode,
                max_rows_by_file=rows_chunk,
                use_threads=True,
                index=False,
                dataset=True,
                schema_evolution=True,
                dtype=schemas[table_name],
                glue_table_settings=wr.typing.GlueTableSettings(
                    columns_comments=column_comments[table_name]
                ),
                escapechar="\\",
                boto3_session=boto3_session,
            )
        elif file_type == "json":
            wr.s3.to_json(
                df=ingested_df,
                path=path,
                database="datalake_raw",
                table=table_name,
                use_threads=True,
                lines=True,
                date_format="iso",
                orient="records",
                index=False,
                dataset=True,
                boto3_session=boto3_session,
            )
        else:
            raise ValueError(
                "Invalid file_type. Supported types are 'csv', 'json', and 'parquet'."
            )

        logger.info(f"[Sucess]: Uploaded to s3://{target_bucket_name}/{table_name}/")
    except Exception as e:
        logger.error("Athena schema:  %s", schemas[table_name])
        logger.error(
            msg=f"Failed uploading to S3 path s3://{target_bucket_name}/{table_name}/"
        )
        logger.error(msg=f"Exception occurred {e}")

        try:
            logger.info("Writing to fallback...")
            fallback_path = f"s3://{target_bucket_name}/{table_name}_fallback/"
            wr.s3.to_json(
                df=ingested_df,
                path=fallback_path,
                partition_cols=["date"],
                mode="append",
                lines=True,
                date_format="iso",
                use_threads=True,
                dataset=True,
                index=False,
                orient="records",
                boto3_session=boto3_session,
            )
            logger.error(
                f"Failed writing to {path}, Success fallback writing to {fallback_path}"
            )
            raise e
        except Exception as e2:
            logger.error(f"Failed fallback with exception: {e2}")
            raise e2


# Deprecated function, please use raw_load_to_s3() instead.
def write_to_s3(
    tempdf,
    athena_table,
    database,
    schema,
    comments,
    s3_bucket=None,
    mode="overwrite_partitions",
    partition_cols=["year", "month", "day"],
):
    """
    writes dataframe to s3 and registers table
    and schema to glue data catalog.

    If fails to writes a backup file as csv.

    AWS Data Wrangler writing to S3
    :param tempdf: Pandas DF to write to S3
    :param athena_table: Table to write to
    :param database: s3 bucket db name
    :param schema: Athena Schema
    :param comments: column comments

    """
    if s3_bucket is None:
        s3_bucket = os.environ["S3_RAW"]

    logger.info("Uploading to S3 bucket:  %s", s3_bucket)
    logger.info("Pandas DF shape:  %s", tempdf.shape)
    path = "s3://" + s3_bucket + "/" + athena_table + "/"
    logger.info("Uploading to S3 location:  %s", path)

    try:
        # issue write command to s3
        res = wr.s3.to_parquet(
            df=tempdf,
            path=path,
            index=False,
            dataset=True,
            database=database,
            table=athena_table,
            mode=mode,
            compression="snappy",
            schema_evolution="true",
            partition_cols=partition_cols,
            dtype=schema,  # athena_schema,
            glue_table_settings=wr.typing.GlueTableSettings(columns_comments=comments),
        )
        return res, path
    except Exception as e:
        logger.error("Athena schema:  %s", schema)
        logger.error("Failed uploading to S3 location:  %s", path)
        logger.error("Exception occurred:  %s", e)

        try:
            logger.info("Writing to fallback...")
            fallback_write_to_s3(tempdf, athena_table, s3_bucket)
        except Exception as e2:
            logger.error(f"Failed fallback with Exception: {e2}")

        return e


def get_actual_dtypes(df) -> dict:
    """Takes a target dataframe, returns the schemas dict
    to be used while creating aws glue table,
    data types references from https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    """
    result_dict = {}
    for column_name in df.columns:
        column_values = (
            df[column_name].replace("None", None).replace("", None).dropna().astype(str)
        )
        try:
            if "date" not in column_name.lower():
                column_values = pd.Series(
                    [
                        ast.literal_eval(entry.capitalize())
                        for entry in column_values.values
                    ]
                )
            else:
                raise Exception

        except Exception:
            try:
                column_values = pd.Series(
                    [pd.to_datetime(entry) for entry in column_values.values]
                )
            except Exception:
                pass

        try:
            if pd.api.types.is_integer_dtype(column_values):
                max_value = column_values.max()
                if max_value >= -(2**31) and max_value <= (2**31 - 1):
                    athena_dtype = "int"
                else:
                    athena_dtype = "bigint"
            elif pd.api.types.is_float_dtype(column_values):
                max_value = column_values.max()
                if str(max_value.dtype) == "float32":
                    athena_dtype = "float"
                else:
                    athena_dtype = "double"
            elif pd.api.types.is_bool_dtype(column_values):
                athena_dtype = "boolean"
            elif (
                pd.api.types.is_datetime64_any_dtype(column_values)
                and column_name != "date"
            ):
                if column_values.equals(column_values.dt.normalize()):
                    athena_dtype = "date"
                else:
                    athena_dtype = "timestamp"

            else:
                athena_dtype = "string"

        except Exception:
            athena_dtype = "string"

        result_dict[column_name] = athena_dtype

    return result_dict


def apply_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Apply specified data types to the columns of a DataFrame based on the input schema

    Args:
        df (pd.DataFrame): DataFrame with generic data types.
        schema (dict): athena df schema containing columns' dtypes

    Returns:
        pd.DataFrame: DataFrame with data types specified in the schema.
    """

    # Mapping schema types to pandas dtypes
    schema_type_mapping = {
        "int": "int32",
        "bigint": "int64",
        "string": "string[python]",
        "timestamp": "datetime64[ns]",
        "double": "float64",
        "boolean": "bool",
        "date": "datetime64[ns]",
    }

    for column, dtype in schema.items():
        if column in df.columns:
            pandas_dtype = schema_type_mapping.get(dtype, "string[python]")
            if dtype in ["timestamp", "date"] and column != "date":
                df[column] = apply_iso_format(df[column])
            elif dtype in ["int", "bigint", "double"]:
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
            else:
                df[column] = df[column].astype(pandas_dtype)

    return df


def apply_iso_format(timestamp_column: pd.Series) -> pd.Series:
    """
    Apply ISO format to a timestamp column, trying multiple formats for each record.

    Args:
        timestamp_column (pd.Series): Series with timestamp data to be processed.

    Returns:
        pd.Series: Series with ISO formatted timestamps.
    """
    # Define the list of date formats to try
    date_formats = [
        "ISO8601",  # ISO8601 format
        "%Y%m%d%H%M",  # Ex: 202409090450
        "%b %d, %Y, %I:%M:%S %p",  # Ex: Sep 16, 2024, 03:41:17 AM
        "%m/%d/%Y %I:%M:%S %p",  # Ex: 7/30/2024 6:27:00 PM
        "%Y-%m-%d %H:%M:%S",  # Ex: 2024-07-30 18:27:00
        "%d-%m-%Y %H:%M:%S",  # Ex: 30-07-2024 18:27:00
    ]

    def parse_date(date_str):
        for date_format in date_formats:
            try:
                return pd.to_datetime(
                    date_str, format=date_format, utc=True, errors="raise"
                )
            except (ValueError, TypeError):
                continue
        raise ValueError(
            f"Error processing date {date_str} in {timestamp_column.name}: Unable to parse date with provided formats"
        )

    return timestamp_column.apply(parse_date)


def get_salesforce_data(data, salesforce_api):
    data_list = []
    current_data = data  # Assuming 'data' is the initial response

    while True:
        # Append the current batch of records to your list
        data_list.append(current_data["records"])

        # Check if there's a next page to fetch
        if "nextRecordsUrl" in current_data:
            # Fetch the next page
            next_url = current_data["nextRecordsUrl"] + "/"
            current_data = salesforce_api.get(
                endpoint=next_url,
                filter_objects=["nextRecordsUrl", "records"],
                clean=True,
            )
        else:
            # No more pages to fetch, break out of the loop
            break
    return data_list


def get_salesforce_df(data_list):
    flattened_data = [flatten(record) for sublist in data_list for record in sublist]
    # Convert the flattened list of records into a DataFrame
    df = pd.DataFrame(flattened_data)
    df["date"] = date.today().strftime("%Y%m%d")
    df["timestamp_extracted"] = datetime.now(timezone.utc)
    columns_to_drop = [
        "attributes_type",
        "attributes_url",
    ]

    # Drop columns only if they exist in the DataFrame
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    return df


# Convert camelCase to snake_case
def camelcase_to_snake_case(df):
    columns = []
    for col in df.columns:
        x = re.sub("(?!^)([A-Z]+)", r"_\1", col).lower()
        columns.append(x)
    df.columns = columns
    return df


# If you want to fill milliseconds with zeros in timestamp columns
def fill_milliseconds(df, table_name, schemas):
    """If you are facing timestamp format errors while loading to S3
    as some entries does not include the milliseconds part.
    Takes a target dataframe, table_name and schemas and returns the modified df
    with missing milliseconds filled with zeros.
    """

    logger.info("Filling milliseconds if found..")

    for column_name in schemas[table_name].keys():
        dtype = schemas[table_name][column_name]
        if dtype == "timestamp":
            for i, entry in enumerate(df[column_name]):
                try:
                    # Format the datetime object with milliseconds included
                    entry_datetime = datetime.strptime(entry, "%Y-%m-%dT%H:%M:%S")
                    df.loc[i, column_name] = entry_datetime.strftime(
                        "%Y-%m-%dT%H:%M:%S.%f"
                    )
                    logger.info("Modified: " + df.loc[i, column_name])
                except (ValueError, TypeError):
                    pass
    return df


def contains_arabic(text):
    # Regular expression to match Arabic characters
    arabic_pattern = re.compile(
        r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]+"
    )
    return bool(arabic_pattern.search(text))


def generate_data_catalog(df, table_name, existing_comments):
    # Step 1: Extract Column Information
    columns_info = []
    for column in df.columns:
        column_info = {"column_name": column, "data_type": str(df[column].dtype)}
        columns_info.append(column_info)

    def generate_comment(column_name):
        return f"This is the {column_name.replace('_', ' ')} column."

    column_comments = {table_name: {}}

    schemas = {table_name: {}}

    for col in columns_info:
        column_name = col["column_name"]
        data_type = col["data_type"]
        # Transform data types
        if column_name == "timestamp_extracted":
            data_type = "timestamp"
        elif data_type == "object":
            data_type = "string"
        elif data_type == "int64":
            data_type = "float"
        elif data_type == "float64":
            data_type = "float"
        elif data_type == "bool":
            data_type = "boolean"

        column_comments[table_name][column_name] = existing_comments.get(
            column_name, generate_comment(column_name)
        )
        schemas[table_name][column_name] = data_type

    return column_comments, schemas


# This version of the function simplifies the process by removing unnecessary arguments like index=False and GlueTableSettings.
def raw_write_to_s3(
    ingested_df: pd.DataFrame,
    table_name: str,
    env: Literal["sandbox", "alpha", "beta", "prod"],
    file_type: Literal["csv", "json", "parquet"],
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

    if schemas[table_name]:
        ingested_df.columns = schemas[table_name].keys()

    logger.info("Dataframe shape:  %s", ingested_df.shape)
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
