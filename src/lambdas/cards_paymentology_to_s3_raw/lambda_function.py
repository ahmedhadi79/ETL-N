"""Module for processing card paymentology data"""

import logging
import os
import re
import ast
import json
from typing import Any, Dict, List, Optional

import awswrangler as wr
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Case1: Execution inside AWS Lambda
if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
    from data_catalog import schemas, column_comments

# Case2: Local or test execution
else:
    from .data_catalog import column_comments, schemas


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _json_safe(obj: Any) -> Any:
    """Coerce any object into a JSON-serializable structure."""
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    else:
        try:
            return json.dumps(obj, default=str)
        except Exception as e:
            # Fallback
            return str(e)


def get_s3_bucket_files(src_s3_bucket: str, src_s3_key: str) -> List[Dict[str, str]]:
    """
    Retrieves a list of files in the S3 bucket/prefix.

    :param src_s3_bucket: The bucket name
    :param src_s3_key: The prefix (folder path)
    :return: List of dicts with 'full_path' and 'filename'
    """
    logger.info("get_s3_bucket_files: %s/%s", src_s3_bucket, src_s3_key)
    all_files: List[Dict[str, str]] = []

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=src_s3_bucket, Prefix=src_s3_key)

    for page in pages:
        for obj in page.get("Contents") or []:
            key = obj.get("Key", "")
            file_n = key.split("/")[-1]
            if len(file_n) > 4:
                all_files.append(
                    {"full_path": f"{src_s3_bucket}/{key}", "filename": file_n}
                )

    logger.info(">> Found %d files for processing.", len(all_files))
    return all_files


def write_to_s3(
    output_df: pd.DataFrame,
    athena_table: str,
    filename_prefix: str,
    s3_bucket: Optional[str] = None,
) -> Dict[str, Any]:
    """Writes data to S3 using awswrangler and returns a JSON-safe dict result."""
    if s3_bucket is None:
        s3_bucket = os.environ["S3_RAW"]

    logger.info("Uploading to S3 bucket: %s", s3_bucket)
    logger.info("Pandas DataFrame Shape: %s", output_df.shape)

    path = f"s3://{s3_bucket}/{athena_table}"
    logger.info("Uploading to S3 location: %s", path)

    try:
        res = wr.s3.to_csv(
            df=output_df,
            path=path,
            index=False,
            dataset=True,
            database="datalake_raw",
            table=athena_table,
            mode="append",
            filename_prefix=filename_prefix,
            schema_evolution=True,
            dtype=schemas[athena_table],
            columns_comments=column_comments[athena_table],
            escapechar="\\",
        )

        return {
            "ok": True,
            "table": athena_table,
            "dest_path": path,
            "result": _json_safe(res),
        }
    except Exception as e:
        logger.exception("Failed uploading to S3 location: %s", path)
        return {
            "ok": False,
            "table": athena_table,
            "dest_path": path,
            "error": str(e),
        }


def lambda_handler(event, context):
    """
    AWS Lambda handler. Processes new Paymentology files and appends them to per-type Athena tables.
    Always returns a JSON-serializable payload.
    """
    sftp_bucket = os.environ["SFTP_S3_BUCKET"]
    sftp_prefix = os.environ["SFTP_S3_KEY"]
    s3_raw_bucket = os.environ["S3_RAW"]

    # Read previously processed list (if exists)
    processed_list: List[str] = []
    s3_res = boto3.resource("s3")
    processed_key = "paymentology_processed_files.txt"
    try:
        obj = s3_res.Object(sftp_bucket, processed_key)
        processed_files = obj.get()["Body"].read().decode("utf-8")
        processed_list = [
            s.strip("\r") for s in re.split(r"\n", processed_files) if s.strip()
        ]
    except ClientError:
        logger.info("No files logged in process list")

    # Check event for manual mode
    to_be_processed_list = ast.literal_eval(event.get("to_be_processed_keys", "[]"))
    if not to_be_processed_list:
        all_files = get_s3_bucket_files(sftp_bucket, sftp_prefix)

        # Single-pass build of full paths from discovery
        paymentology_files: List[str] = [item["full_path"] for item in all_files]

        # Determine new work
        to_be_processed_list = sorted(set(paymentology_files) - set(processed_list))
    logger.info(">> New files to be processed: %s", len(to_be_processed_list))

    main_file_mask = "BB2 DIGITAL AND TECHNOLOGY SERVICES LTD_"
    results: List[Dict[str, Any]] = []
    completed_list: List[str] = []

    groups = ["Fees", "Presentments", "Interchange"]

    for tp in groups:
        logger.info("Started work with files for the group: %s", tp)
        for f in to_be_processed_list:
            # f looks like '<bucket>/<prefix>/filename.csv'  (because we constructed full_path that way)
            # We need only the filename for matching + tagging:
            filename = f.split("/")[-1]
            if f"{main_file_mask}{tp}" in filename:
                logger.info(
                    ">> File %s matched to the group %s, processing...", filename, tp
                )
                # Convert 'bucket/prefix/file' to s3://bucket/prefix/file for reading
                s3_uri = f"s3://{f}"
                df = wr.s3.read_csv(path=s3_uri)
                df["source_file"] = filename

                # lowercase df.columns
                df.columns = [col.lower() for col in df.columns]

                # Get schema columns in their defined order
                schema_columns = list(
                    schemas[f"cards_paymentology_{tp.lower()}"].keys()
                )

                # Find extra columns
                extra_columns = set(df.columns) - set(schema_columns)
                if extra_columns:
                    logger.warning(
                        "Dropping extra columns not in schema: %s", extra_columns
                    )

                # Keep only schema columns in the correct order
                existing_schema_columns = [
                    col for col in schema_columns if col in df.columns
                ]
                df = df[existing_schema_columns]

                write_result = write_to_s3(
                    output_df=df,
                    s3_bucket=s3_raw_bucket,
                    filename_prefix=filename.replace(main_file_mask, "").replace(
                        ".csv", "__"
                    ),
                    athena_table=f"cards_paymentology_{tp.lower()}",
                )
                if write_result.get("ok"):
                    completed_list.append(f)
                results.append(
                    {
                        "group": tp,
                        "file": f,
                        "filename": filename,
                        **write_result,
                    }
                )

    # Update processed file log (keep unique & sorted)
    new_list = sorted(set(completed_list).union(processed_list))
    try:
        obj = s3_res.Object(sftp_bucket, processed_key)
        obj.put(Body="\n".join(new_list))
    except Exception as e:
        # Do not fail the Lambda response marshaling because of a logging write;
        # just record the issue in the JSON.
        results.append({"ok": False, "stage": "update_processed_log", "error": str(e)})

    logger.info("Done")

    # Compact, JSON-safe summary return
    summary = {
        "processed_count": len(completed_list),
        "queued_count": len(to_be_processed_list),
        "processed_log_size": len(new_list),
        "results": _json_safe(results),
    }
    return summary
