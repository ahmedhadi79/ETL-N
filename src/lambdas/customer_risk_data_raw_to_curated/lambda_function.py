import logging
import os
from datetime import date
from typing import Any

import awswrangler as wr
import pandas as pd

import config
import data_catalog

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
    partition_cols: Any,
    s3_bucket: str = None,
) -> dict:
    if s3_bucket is None:
        s3_bucket = os.environ["S3_CURATED"]

    logger.info(f"Uploading to S3 bucket: {s3_bucket}")
    logger.info(f"Pandas DataFrame Shape: {output_df.shape}")
    path = f"s3://{s3_bucket}/{athena_table}/"
    logger.info("Uploading to S3 location:  %s", path)

    if partition_cols is not None:
        try:
            res = wr.s3.to_parquet(
                df=output_df,
                path=path,
                index=False,
                dataset=True,
                database=database_name,
                table=athena_table,
                mode="overwrite_partitions",
                schema_evolution="true",
                compression="snappy",
                partition_cols=partition_cols,
                dtype=data_catalog.schemas[athena_table],
                glue_table_settings=wr.typing.GlueTableSettings(
                    columns_comments=data_catalog.column_comments[athena_table]
                ),
            )

            return res

        except Exception as e:
            logger.error(f"Failed uploading to S3 location:  {path}")
            logger.error(f"Exception occurred:  {e}")

            return e
    else:
        try:
            res = wr.s3.to_csv(
                df=output_df,
                path=path,
                index=False,
                dataset=True,
                database=database_name,
                table=athena_table,
                mode="overwrite",
                schema_evolution="true",
            )

            return res

        except Exception as e:
            logger.error(f"Failed uploading to S3 location:  {path}")
            logger.error(f"Exception occurred:  {e}")

            return e


def lambda_handler(event, context):
    input_database = "datalake_raw"
    partition_cols = ["date"]

    sql_path = config.config["customer_risk_form_data_raw_to_curated"]["sql_path"]

    athena_table = "dynamo_sls_customer_risk_form"
    res, df = read_sql_from_athena(sql_path, input_database)

    df["job_run_date"] = date.today().strftime("%Y%m%d")

    if res:
        column_names_series = df["column_name"]
        columns = [column[:] for column in column_names_series]
        column_str = ",".join(columns)
        final_query = (
            "select "
            + column_str
            + '  FROM "datalake_raw"."dynamo_sls_customer_risk_form"'
        )

        df_data = wr.athena.read_sql_query(
            sql=final_query,
            database=input_database,
            workgroup="datalake_workgroup",
            ctas_approach=False,
        )

        write_output = write_to_s3(
            output_df=df_data,
            athena_table=athena_table,
            database_name="datalake_curated",
            partition_cols=partition_cols,
        )
        logger.info(f"Result: {write_output}")

    else:
        logger.error("Exiting function...")
