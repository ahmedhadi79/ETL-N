import logging
import os
from datetime import datetime
import awswrangler as wr
import pandas as pd

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


def write_to_s3(tempdf, athena_table, s3_bucket=None):
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
    today_date = "date =" + str(datetime.now())[:10]
    path = f"s3://{s3_bucket}/{athena_table}/{today_date}/"
    logger.info("Uploading to S3 location:  %s", path)

    try:
        # issue write command to s3
        res = wr.s3.to_csv(
            df=tempdf,
            path=path,
            dataset=True,
            mode="overwrite",
        )
        return res, path
    except Exception as e:
        logger.error("Failed uploading to S3 location:  %s", path)
        logger.error("Exception occurred:  %s", e)


def process_income_wealth_data():
    sql_path = "customer_curated.sql"
    input_database = "datalake_curated"
    res, result_df = read_sql_from_athena(sql_path, input_database)
    logger.info("Data Frame read from Athena with Shape: ")

    if res:
        athena_table = "income_wealth_DataLake"
        res, path = write_to_s3(result_df, athena_table)

        file_name = res["paths"][0]
        logger.info("Final file written in aws s3 under path :  %s", path)
        logger.info(
            "Final file written in aws s3 under path with full name :  %s", file_name
        )
    else:
        logger.error("Exiting function...")


def lambda_handler(event, context):
    process_income_wealth_data()
