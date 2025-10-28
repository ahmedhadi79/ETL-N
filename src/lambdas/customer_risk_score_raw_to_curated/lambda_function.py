import logging
import os
from datetime import datetime, timezone
import awswrangler as wr
import pandas as pd
import config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_sql_from_athena(sql_path: str, input_database: str) -> pd.DataFrame:
    logger.info("Reading sql file... ")
    with open(sql_path, "r") as sql_file:
        sql = sql_file.read()

    logger.info("Reading from Athena... ")
    try:
        df = wr.athena.read_sql_query(
            sql=sql,
            database=input_database,
            workgroup="datalake_workgroup",
            ctas_approach=False,
        )

        return True, df

    except Exception as e:
        logger.error("Failed reading from Athena")
        logger.error(f"Exception occurred:  {e}")
        return False, None


def write_to_s3(
    output_df: pd.DataFrame,
    athena_table: str,
    database_name: str,
    partition_cols: list,
    s3_bucket: str,
    write_mode: str,
) -> dict:

    logger.info(f"Uploading to S3 bucket: {s3_bucket}")
    logger.info(f"Pandas DataFrame Shape: {output_df.shape}")
    path = f"s3://{s3_bucket}/{athena_table}/"
    logger.info("Uploading to S3 location:  %s", path)

    try:
        res = wr.s3.to_parquet(
            df=output_df,
            path=path,
            index=False,
            dataset=True,
            database=database_name,
            table=athena_table,
            mode=write_mode,
            schema_evolution="true",
            compression="snappy",
            partition_cols=partition_cols,
            dtype=config.config["customer_risk_score_data_raw_to_curated"]["catalog"],
        )

        return res

    except Exception as e:
        logger.error(f"Failed uploading to S3 location:  {path}")
        logger.error(f"Exception occurred:  {e}")

        return e


def lambda_handler(event, context):
    # STEP1: env vars
    s3_bucket = os.environ["S3_CURATED"]
    wr_write_mode = os.environ["WRANGLER_WRITE_MODE"]

    # STEP2: athena query raw layer
    input_database = "datalake_raw"
    sql_path = config.config["customer_risk_score_data_raw_to_curated"]["sql_path"]
    athena_table = "dynamo_sls_riskscore"
    res, df = read_sql_from_athena(sql_path, input_database)

    # STEP3: meta columns
    partition_cols = ["year", "month", "day"]
    df[partition_cols] = df["date"].astype(str).str.split("-", expand=True)
    df["timestamp_extracted"] = datetime.now(timezone.utc)

    if res:
        write_output = write_to_s3(
            output_df=df,
            athena_table=athena_table,
            database_name="datalake_curated",
            partition_cols=partition_cols,
            s3_bucket=s3_bucket,
            write_mode=wr_write_mode,
        )
        logger.info(f"Result: {write_output}")

    else:
        logger.error("Exiting function...")
