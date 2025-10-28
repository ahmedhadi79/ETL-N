import io
import json
import logging
import os
from datetime import date
from datetime import datetime

import awswrangler as wr
import boto3
import pandas as pd
import requests
import data_catalog

# import currencycloud
# from currencycloud.errors import ApiError
# from numpy import dtype
# import numpy as np
# import base64

# from flatten_json import flatten
# from botocore.exceptions import ClientError
# import time


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager
    :param secret_name: The key to retrieve
    :return: The value of the secret
    """
    logger.info("Retrieving :  %s", secret_name)
    try:
        secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value["SecretString"])
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        print(e)
        return False


# Configure and instantiate the Client
base_url = "https://devapi.currencycloud.com/"
token_url = "v2/authenticate/api"
fullurl = base_url + token_url
secret_name = "data/currencycloud/api"


api_credentials = get_secret(secret_name)
login_id = list(api_credentials.keys())[0]
api_key = api_credentials[login_id]

data = {"login_id": login_id, "api_key": api_key}
response = requests.post(fullurl, data=data)
logger.info("Checking API Authentication: %s", response)

token = response.json()["auth_token"]

balance_url = "v2/balances/find"
accounts_url = "v2/accounts/find"
beneficiaries_url = "v2/beneficiaries/find"
contacts_url = "v2/contacts/find"
conversions_url = "v2/conversions/find"
payments_url = "v2/payments/find"
transactions_url = "v2/transactions/find"
transfers_url = "v2/transfers/find"

rates_url = "v2/rates/find"
funding_url = "v2/funding_accounts/find"

currency_cloud_urls = [
    balance_url,
    accounts_url,
    beneficiaries_url,
    contacts_url,
    conversions_url,
    payments_url,
    transactions_url,
    transfers_url,
]

ccys = [
    {"currency": "EUR"},
    {"currency": "GBP"},
    {"currency": "USD"},
    {"currency": "KWD"},
    {"currency": "SAR"},
    {"currency": "AED"},
    {"currency": "CAD"},
    {"currency": "CNY"},
    {"currency": "JPY"},
    {"currency": "CHF"},
    {"currency": "NZD"},
    {"currency": "HKD"},
]

ccy_pairs = [
    {"currency_pair": "EURUSD"},
    {"currency_pair": "GBPUSD"},
    {"currency_pair": "USDJPY"},
    {"currency_pair": "AUDUSD"},
    {"currency_pair": "EURGBP"},
    {"currency_pair": "USDCAD"},
    {"currency_pair": "USDCHF"},
    {"currency_pair": "NZDCHF"},
    {"currency_pair": "USDCNY"},
    {"currency_pair": "USDHKD"},
]


def parse_currency_cloud(base_url, url, token):
    """
    Extract parse and enrich data from a single non-parameterized endpoint in the currency cloud API.
    Construct the API endpoints, extract data & source names from all pages and store results in the empty dataframe
    """
    extract_url = base_url + url
    logger.info(extract_url)
    response = requests.get(extract_url, headers={"X-Auth-Token": token})
    data = response.json()
    pages = data["pagination"]
    total_pages = pages["total_pages"]
    if total_pages < 1:
        total_pages = 1
    else:
        total_pages
    data_name = url.split("/")[1].strip()
    df = pd.json_normalize(data[data_name])
    df = pd.DataFrame(columns=df.columns)
    # pagination of data returned from API
    for i in range(total_pages):
        i = i + 1
        page = f"?page={i}"
        page_url = extract_url + page
        response = requests.get(
            page_url,
            headers={
                "X-Auth-Token": token,
            },
        )
        page_data = response.json()

        page_data[data_name]
        df1 = pd.json_normalize(page_data[data_name])
        df = pd.concat([df, df1])
        # enrich with metadata and enforce datatypes on dataframe
        df["timestamp_extracted"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ]
        df["record_format"] = "application/json"
        df["extract_url"] = extract_url
        currencycloud = "currencycloud_"
        table_name = currencycloud + extract_url.split("/")[-2]
        df["table_name"] = table_name
        df["date"] = date.today().strftime("%Y%m%d")  # str(date.today())
        df = df.infer_objects()

    df = df.reset_index()
    df = df.drop("index", axis=1)
    # correct nulls to ensure correct datatypes on dataframe
    for col in df.columns:
        if (
            df[col].isnull().values.all()
            or df[col].values.all() == ""
            or df[col].values.all() == []
        ):
            empty_col = 1
        else:
            empty_col = 0
        if df[col].dtype == "object" and empty_col == 0:
            try:
                df[col] = pd.to_numeric(df[col], downcast="integer")
            except Exception:
                try:
                    df[col] = pd.to_datetime(df[col], format="%Y%m%d")
                except Exception:
                    df[col] = df[col]

    return df, table_name


def parse_funding_cloud(base_url, funding_url, ccys, token):
    """
    Extract parse and enrich funding account details from currency cloud API.
    Construct the API endpoint, extract data & source name and store results in the empty dataframe
    """
    extract_url = base_url + funding_url
    data_name = funding_url.split("/")[1].strip()
    df_final = pd.DataFrame()

    # Get funding data for all pages and all currencies inscope
    for ccy in ccys:
        logger.info(extract_url + "_" + ccy["currency"])
        response = requests.get(
            extract_url, headers={"X-Auth-Token": token}, params=ccy
        )
        data = response.json()
        pages = data["pagination"]
        total_pages = pages["total_pages"]
        if total_pages < 1:
            total_pages = 1
        else:
            total_pages
        data_name = funding_url.split("/")[1].strip()
        df = pd.json_normalize(data[data_name])
        df = pd.DataFrame(columns=df.columns)

        for i in range(total_pages):
            i = i + 1
            page = f"?page={i}"
            page_url = extract_url + page
            response = requests.get(
                page_url, headers={"X-Auth-Token": token}, params=ccy
            )
            page_data = response.json()
            page_data[data_name]
            df1 = pd.json_normalize(page_data[data_name])
            df = pd.concat([df, df1])

            # enrich with metadata and enforce datatypes on dataframe
            df["timestamp_extracted"] = datetime.utcnow().strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]
            df["record_format"] = "application/json"
            df["extract_url"] = extract_url
            currencycloud = "currencycloud_"
            table_name = currencycloud + extract_url.split("/")[-2]
            df["table_name"] = table_name
            df["date"] = date.today().strftime("%Y%m%d")  # str(date.today())
            df = df.infer_objects()

        df = df.reset_index()
        df = df.drop("index", axis=1)
        # correct nulls to ensure correct datatypes on dataframe
        for col in df.columns:
            if (
                df[col].isnull().values.all()
                or df[col].values.all() == ""
                or df[col].values.all() == []
            ):
                empty_col = 1
            else:
                empty_col = 0
            if df[col].dtype == "object" and empty_col == 0:
                try:
                    df[col] = pd.to_numeric(df[col], downcast="integer")
                except Exception:
                    try:
                        df[col] = pd.to_datetime(df[col], format="%Y%m%d")
                    except Exception:
                        df[col] = df[col]

        df_final = pd.concat([df_final, df])

    logger.info(df_final.dtypes)

    return df_final, table_name


def parse_rates_cloud(base_url, rates_url, ccy_pairs, token):
    """
    Extract parse and enrich exchange rate details from currency cloud API.
    Construct the API endpoint, extract data & source name and store results in the empty dataframe
    """
    extract_url = base_url + rates_url
    data_name = rates_url.split("/")[1].strip()
    df = pd.DataFrame()
    # Get exchage rates data all currency pairs inscope
    for pair in ccy_pairs:
        logger.info(extract_url + "_" + pair["currency_pair"])
        response = requests.get(
            extract_url, headers={"X-Auth-Token": token}, params=pair
        )
        page_data = response.json()
        page_data[data_name]
        df1 = pd.json_normalize(page_data[data_name])
        df = pd.concat([df, df1], axis=1)
    # enrich with metadata and enforce datatypes on dataframe
    df["timestamp_extracted"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    df["record_format"] = "application/json"
    df["extract_url"] = extract_url
    currencycloud = "currencycloud_"
    table_name = currencycloud + extract_url.split("/")[-2]
    df["table_name"] = table_name
    df["date"] = date.today().strftime("%Y%m%d")  # str(date.today())
    df = df.infer_objects()
    df = df.reset_index()
    df = df.drop("index", axis=1)
    # correct nulls to ensure correct datatypes on dataframe
    for col in df.columns:
        if (
            df[col].isnull().values.all()
            or df[col].values.all() == ""
            or df[col].values.all() == []
        ):
            empty_col = 1
        else:
            empty_col = 0
        if df[col].dtype == "object" and empty_col == 0:
            try:
                df[col] = pd.to_numeric(df[col], downcast="integer")
            except Exception:
                try:
                    df[col] = pd.to_datetime(df[col], format="%Y%m%d")
                except Exception:
                    df[col] = df[col]

    logger.info(df.dtypes)

    return df, table_name


def get_currencycloud_schema(df):
    athena_schema = {}
    for col in df.columns:
        if df.dtypes[col] == "object":
            athena_schema[col] = "string"
        elif str(df.dtypes[col])[:8] == "datetime":
            athena_schema[col] = "timestamp"
        elif str(df.dtypes[col])[:5] == "float":
            athena_schema[col] = "double"
        elif str(df.dtypes[col])[:3] == "int":
            athena_schema[col] = "int"
        elif df.dtypes[col] == "bool":
            athena_schema[col] = "boolean"
        else:
            athena_schema[col] = "string"

    athena_schema["date"] = "date"

    dt = datetime.utcnow()
    df["date"] = dt.strftime("%Y%m%d")

    logger.info(athena_schema)
    print(athena_schema)
    return df, athena_schema


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


def write_to_s3(tempdf, athena_table, athena_schema, partition_columns, s3_bucket=None):
    """
    AWS Data Wrangler writing to S3
    :param tempdf: Pandas DF to write to S3
    :param athena_table: Table to write to
    :param athena_schema: Athena Schema
    :param partition_columns: The columns to use for partitioning the data
    :return: result
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
            database="datalake_raw",
            table=athena_table,
            mode="append",  # "overwrite_partitions",
            compression="snappy",
            schema_evolution="true",
            partition_cols=partition_columns,
            # columns_comments=column_comments[athena_table],
            dtype=data_catalog.schemas[athena_table],  # athena_schema,
            glue_table_settings=wr.typing.GlueTableSettings(
                columns_comments=data_catalog.column_comments[athena_table]
            ),
        )
        return res, path
    except Exception as e:
        logger.error("Athena schema:  %s", athena_schema)
        logger.error("Failed uploading to S3 location:  %s", path)
        logger.error("Exception occurred:  %s", e)

        try:
            logger.info("Writing to fallback...")
            fallback_write_to_s3(tempdf, athena_table, s3_bucket)
        except Exception as e2:
            logger.error(f"Failed fallback with Exception: {e2}")

        return e


def get_currency_cloud_data(currency_cloud_urls):
    for url in currency_cloud_urls:
        logger.info("Request data from:  %s", url)
        df, target_athena_glue_table = parse_currency_cloud(base_url, url, token)
        final_df, athena_schema = get_currencycloud_schema(df)
        partition_columns = ["date"]
        logger.info("Now writing to: %s", target_athena_glue_table)
        logger.info("Initiating write to s3 routine...")

        res = write_to_s3(
            final_df, target_athena_glue_table, athena_schema, partition_columns
        )
        logger.info("Result:  %s", res)
        logger.info("Finished processing event.")
        logger.info("Finished writing to: %s", target_athena_glue_table)

    df, target_athena_glue_table = parse_funding_cloud(
        base_url, funding_url, ccys, token
    )
    final_df, athena_schema = get_currencycloud_schema(df)
    logger.info("Now writing to: %s", target_athena_glue_table)
    logger.info("Initiating write to s3 routine...")

    res = write_to_s3(
        final_df, target_athena_glue_table, athena_schema, partition_columns
    )
    logger.info("Result:  %s", res)
    logger.info("Finished processing event.")
    logger.info("Finished writing to: %s", target_athena_glue_table)

    df, target_athena_glue_table = parse_rates_cloud(
        base_url, rates_url, ccy_pairs, token
    )
    final_df, athena_schema = get_currencycloud_schema(df)
    logger.info("Now writing to: %s", target_athena_glue_table)
    logger.info("Initiating write to s3 routine...")

    res = write_to_s3(
        final_df, target_athena_glue_table, athena_schema, partition_columns
    )
    logger.info("Result:  %s", res)
    logger.info("Finished processing event.")
    logger.info("Finished writing to: %s", target_athena_glue_table)


def lambda_handler(event, context):
    get_currency_cloud_data(currency_cloud_urls)
    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
