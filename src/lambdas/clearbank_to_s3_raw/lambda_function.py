import os
import json
import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Optional
import boto3
import pandas as pd
from utils import (
    logger,
    CustomError,
    get_secret,
    S3Utils,
    flatten_dict_cols,
    s3_write_raw,
    build_table_name,
)


if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
    from api_client import APIClient


async def fetch_page(
    cb_client,
    endpoint: str,
    page_number: int,
    page_size: int,
    filter_object: Optional[str],
):
    q = f"pageNumber={page_number}&pageSize={page_size}"
    try:
        return cb_client.get(
            endpoint=f"{endpoint}?{q}",
            filter_objects=[filter_object] if filter_object else None,
            clean=True,
            flatten=False,
            df=True,
        )
    except Exception as e:
        logger.error(f"fetch_page error p{page_number}: {e}\n{traceback.format_exc()}")
        raise


async def fetch_all_pages(
    cb_client, endpoint: str, page_size: int, filter_object: Optional[str]
):
    page = 1
    dfs = []
    while True:
        df = await fetch_page(cb_client, endpoint, page, page_size, filter_object)
        if df is None or df.empty:
            break
        dfs.append(df)
        page += 1
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# -------- Job: transactions_daily
async def run_transactions_daily(
    cb_client,
    main_account_id,
    cb_table,
    cb_filter_object,
    start_date: Optional[str],
    end_date: Optional[str],
    page_size: int,
    env: str,
):
    # Default to yesterday in UTC if not provided
    if not start_date or not end_date:
        y = (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = end_date = y
    endpoint = f"Accounts/{main_account_id}/{cb_table}"
    # Include the date window in the endpoint if the API expects them as query params:
    endpoint = (
        f"{endpoint}&startDateTime={start_date}T00:00:00.00&endDateTime={end_date}T23:59:59.59"
        if "?" in endpoint
        else f"{endpoint}?startDateTime={start_date}T00:00:00.00&endDateTime={end_date}T23:59:59.59"
    )
    df = await fetch_all_pages(cb_client, endpoint, page_size, cb_filter_object)
    if df.empty:
        logger.info("No transactions returned")
        return
    df = flatten_dict_cols(df).drop(
        columns=["date", "timestamp_extracted"], errors="ignore"
    )
    s3_write_raw(df, build_table_name(cb_table, cb_filter_object), env)


# -------- Job: mandates_delta
async def run_mandates_delta(
    cb_client, main_account_id, page_size: int, batch_size: int, env: str
):
    s3 = S3Utils(boto3.client("s3"))
    bucket = f"bb2-{env}-datalake-raw"
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    t_keys = s3.list_parquet(bucket, f"cb_virtual_accounts/date={today}/")
    y_keys = s3.list_parquet(bucket, f"cb_virtual_accounts/date={yesterday}/")
    t_df = s3.load_parquet(bucket, t_keys)
    y_df = s3.load_parquet(bucket, y_keys)
    t_ids = set(t_df["id"]) if "id" in t_df.columns else set()
    y_ids = set(y_df["id"]) if "id" in y_df.columns else set()
    va_ids = list((t_ids - y_ids) | (y_ids - t_ids))
    if not va_ids:
        logger.info("No VA changes detected")
        return
    # batch fetch mandates
    dfs = []
    base = f"Accounts/{main_account_id}/Virtual"
    for i in range(0, len(va_ids), batch_size):
        for va in va_ids[i : i + batch_size]:
            endpoint = f"{base}/{va}/Mandates?startDateTime={yesterday}T00:00:00.00&endDateTime={today}T23:59:59.59"
            df = await fetch_all_pages(
                cb_client, endpoint, page_size, "directDebitMandates"
            )
            if df is not None and not df.empty:
                df["virtualAccountId"] = va
                dfs.append(df)
        await asyncio.sleep(1)  # friendly throttle
    if not dfs:
        logger.info("No mandates fetched")
        return
    out = pd.concat(dfs, ignore_index=True).drop(
        columns=["date", "timestamp_extracted"], errors="ignore"
    )
    s3_write_raw(out, "cb_directdebit_mandates_temp", env)


# -------- Dispatcher
async def main_async(event):
    env = os.getenv("ENV")
    api_key = os.getenv("CB_API_KEY")
    base_url = os.getenv("CB_BASE_URL")
    main_account_id = os.getenv("MAIN_ACCOUNT_ID")
    token = get_secret(api_key)
    cb_client = APIClient(auth=f"Bearer {token}", base_url=base_url)

    # event-first params, fallback to env
    job_type = (event.get("job_type") or os.getenv("JOB_TYPE") or "").lower()
    cb_table = event.get("cb_table") or os.getenv("CB_TABLE")
    cb_filter_object = event.get("cb_filter_object") or os.getenv("CB_FILTER_OBJECT")
    start_date = event.get("start_date") or os.getenv("START_DATE")
    end_date = event.get("end_date") or os.getenv("END_DATE")
    page_size = int(event.get("page_size") or os.getenv("PAGE_SIZE") or 1000)
    batch_size = int(event.get("batch_size") or os.getenv("BATCH_SIZE") or 50)

    if job_type == "transactions_daily":
        await run_transactions_daily(
            cb_client,
            main_account_id,
            cb_table,
            cb_filter_object,
            start_date,
            end_date,
            page_size,
            env,
        )
    elif job_type == "mandates_delta":
        await run_mandates_delta(cb_client, main_account_id, page_size, batch_size, env)
    else:
        raise CustomError(f"Unknown job_type: {job_type}")


def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event) if isinstance(event, dict) else event}")
    try:
        asyncio.run(main_async(event or {}))
    except Exception as e:
        logger.error(f"Lambda failed: {e}\n{traceback.format_exc()}")
        raise
