import json
import logging
import os
from typing import Optional, Tuple, Iterable, Dict, Any
from urllib.parse import unquote_plus, quote
import re
from datetime import datetime, date

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

S3 = boto3.client("s3")

DEST_BUCKET = os.environ["S3_RAW"]

DEST_SUBFOLDERS = {
    "fees": "paymentology_fees_data/",
    "interchange": "paymentology_interchange_data/",
    "presentments": "paymentology_presentments_data/",
}


def _detect_group_from_filename(filename: str) -> Optional[str]:
    """
    Decide which logical group the file belongs to by filename.
    We check for substrings with underscores, any case:
      _Fees_, _Interchange_, _Presentments_
    """
    name = filename.lower()
    if "_fees_" in name:
        return "fees"
    if "_interchange_" in name:
        return "interchange"
    if "_presentments_" in name:
        return "presentments"
    return None


def _copy_object(
    src_bucket: str, src_key: str, dest_bucket: str, dest_key: str
) -> None:
    S3.copy_object(
        Bucket=dest_bucket,
        Key=dest_key,
        CopySource={"Bucket": src_bucket, "Key": src_key},
        MetadataDirective="COPY",
    )


def _handle_one_s3_event(*, src_bucket: str, src_key_encoded: str) -> Tuple[bool, str]:
    src_key = unquote_plus(src_key_encoded)
    filename = src_key.rsplit("/", 1)[-1]
    logger.info(
        "Processing object: bucket=%s key=%s filename=%s", src_bucket, src_key, filename
    )

    group = _detect_group_from_filename(filename)
    if not group:
        logger.info("No matching group for filename=%s", filename)
        return True, f"Skip: no group match in filename={filename}"

    subfolder = DEST_SUBFOLDERS[group]
    dest_key = f"{subfolder}{filename}"
    logger.info("Detected group=%s, destination key=%s", group, dest_key)

    # Idempotency: skip if already copied
    try:
        S3.head_object(Bucket=DEST_BUCKET, Key=dest_key)
        logger.info(
            "File already exists at destination: s3://%s/%s", DEST_BUCKET, dest_key
        )
        return True, f"Already exists: s3://{DEST_BUCKET}/{dest_key}"
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in (
            "404",
            "NoSuchKey",
            "NotFound",
        ):
            raise

    # Do the copy
    logger.info(
        "Copying object from s3://%s/%s to s3://%s/%s",
        src_bucket,
        src_key,
        DEST_BUCKET,
        dest_key,
    )
    _copy_object(src_bucket, src_key, DEST_BUCKET, dest_key)
    logger.info("Successfully copied object to s3://%s/%s", DEST_BUCKET, dest_key)
    return True, f"Copied to s3://{DEST_BUCKET}/{dest_key}"


def _extract_date_from_filename(filename: str) -> Optional[Tuple[date, date]]:
    """
    Extract a single date or date range from filename:
      - ..._YYYYMMDD.csv
      - ..._YYYYMMDD_to_YYYYMMDD.csv
    Returns (start_date, end_date) or None.
    """
    m = re.search(r"_(\d{8})(?:_to_(\d{8}))?", filename)
    if not m:
        return None

    d1, d2 = m.group(1), m.group(2)

    try:
        start = datetime.strptime(d1, "%Y%m%d").date()
        end = datetime.strptime(d2, "%Y%m%d").date() if d2 else start
        return start, end
    except ValueError:
        return None


def _iter_csv_keys(bucket: str, prefix: str) -> Iterable[str]:
    """
    Yield S3 keys ending with .csv under the given prefix.
    Uses a paginator so it works for 10k+ objects.
    """
    paginator = S3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                yield key


def _run_backfill(
    *,
    src_bucket: str,
    prefix: str,
    start_date: Optional[str] = None,  # "2021-06-14"
    end_date: Optional[str] = None,  # "2021-06-30"
) -> Dict[str, Any]:
    processed = copied = skipped = 0
    results: list[Dict[str, Any]] = []

    # Convert to date objects if provided
    start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    for key in _iter_csv_keys(src_bucket, prefix):
        filename = key.rsplit("/", 1)[-1]
        file_dates = _extract_date_from_filename(filename)

        # Skip if no date info but filters are given
        if (start or end) and not file_dates:
            logger.info("Skipping (no valid date) %s", filename)
            skipped += 1
            continue

        if file_dates:
            file_start, file_end = file_dates
            if start and file_end < start:
                continue
            if end and file_start > end:
                continue

        encoded = quote(key, safe="/")
        ok, msg = _handle_one_s3_event(src_bucket=src_bucket, src_key_encoded=encoded)
        copied += int(ok and msg.startswith("Copied"))
        skipped += int(
            ok and (msg.startswith("Already exists") or msg.startswith("Skip"))
        )
        processed += 1
        results.append({"key": key, "ok": ok, "msg": msg})

    summary = {
        "source": f"s3://{src_bucket}/{prefix}",
        "processed": processed,
        "copied": copied,
        "skipped": skipped,
        "start_date": start_date,
        "end_date": end_date,
    }
    logger.info("Backfill summary: %s", summary)
    return {"summary": summary, "results": results}


def lambda_handler(event, context):
    logger.info("Lambda triggered with event: %s", json.dumps(event)[:500])

    # ---- Direct-invoke backfill mode ----
    if isinstance(event, dict) and "backfill" in event:
        bf = event["backfill"]
        src_bucket = bf["src_bucket"]
        prefix = bf["prefix"]
        start_date = bf.get("start_date")  # "YYYY-MM-DD" or None
        end_date = bf.get("end_date")  # "YYYY-MM-DD" or None

        return _run_backfill(
            src_bucket=src_bucket,
            prefix=prefix,
            start_date=start_date,
            end_date=end_date,
        )

    # ---- Normal SQS/S3 notifications ----
    for record in event.get("Records", []):
        msg_id = record.get("messageId")
        logger.info("Processing SQS record: messageId=%s", msg_id)
        try:
            body = json.loads(record["body"])
            logger.info("Parsed body for messageId=%s: %s", msg_id, str(body)[:300])

            if body.get("Event") == "s3:TestEvent":
                logger.info("Ignoring s3:TestEvent for messageId=%s", msg_id)
                continue

            if "Records" not in body:
                logger.info("Non-S3 body detected for messageId=%s", msg_id)
                continue

            for s3r in body["Records"]:
                if s3r.get("eventSource") != "aws:s3":
                    logger.info("Skipping non-S3 eventSource in messageId=%s", msg_id)
                    continue

                src_bucket = s3r["s3"]["bucket"]["name"]
                src_key = s3r["s3"]["object"]["key"]

                ok, msg = _handle_one_s3_event(
                    src_bucket=src_bucket, src_key_encoded=src_key
                )
                logger.info("Result for messageId=%s: ok=%s info=%s", msg_id, ok, msg)

        except Exception as e:
            logger.exception(
                "Failed processing SQS messageId=%s, error=%s", msg_id, str(e)
            )

    return True
