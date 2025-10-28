import boto3
import config
import logging
import os
import pandas as pd
import awswrangler as wr
import time
from awswrangler import exceptions
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_athena(sql_path: str, input_database: str) -> pd.DataFrame:
    with open(sql_path, "r") as sql_file:
        sql = sql_file.read()
    logger.info("Reading from Athena... ")
    df = wr.athena.read_sql_query(
        sql=sql,
        database=input_database,
        workgroup="datalake_workgroup",
        ctas_approach=False,
    )
    return df


def write_as_csv_s3(data: pd.DataFrame, s3_bucket: str = None):
    try:
        if s3_bucket is None:
            s3_bucket = os.environ.get("S3_CURATED")

        date = datetime.now().strftime("%Y-%m-%d")
        time = datetime.now().strftime("%H-%M-%S")
        s3_key = f"monthly_nomo_report/{date}/{time}/monthly_nomo_report.csv"
        s3_path = f"s3://{s3_bucket}/{s3_key}"

        logger.info(f"Shape: {data.shape}")
        logger.info("Uploading to S3 location: %s", s3_path)

        res = wr.s3.to_csv(
            df=data,
            path=s3_path,
            index=False,
            dataset=False,
        )
        return res
    except exceptions.AWSServiceError as e:
        logger.error("AWS Wrangler Exception occurred: %s", e)
    except Exception as e:
        logger.error("Failed uploading to S3 location: %s", s3_path)
        logger.error("Exception occurred: %s", e)
        exit(1)


def build_and_send_report(report_name: str):
    logger.info(f"Building report {report_name}...")

    sql_path = config.config[report_name]["report_query"]
    df = read_athena(sql_path, "datalake_raw")
    res_s3_csv = write_as_csv_s3(df)
    logger.info("Finished writing to S3 as CSV...")
    logger.info(res_s3_csv)
    # Step#1: Generate attachment
    logger.info("Generate attachment")
    attachment_path = f"/tmp/{report_name}.zip"
    df.to_csv(
        attachment_path,
        index=False,
        compression=dict(method="zip", archive_name=f"{report_name}.csv"),
    )

    # Step#2: Generate email
    logger.info("Generate email")
    msg = MIMEMultipart()
    env_name = os.environ.get("ENV_NAME")
    msg[
        "Subject"
    ] = f'[{env_name.upper()}]: {config.config[report_name]["email_subject"].replace("{DATE}", datetime.now().strftime("%Y-%m-%d"))}'
    msg["From"] = f'do-not-replay <{os.environ["SES_FROM_EMAIL"]}>'
    msg["To"] = ", ".join(config.config[report_name]["report_recipient"])
    msg["Cc"] = ", ".join(config.config[report_name]["cc_recipients"])
    msg.attach(MIMEText("Attached archived report."))

    with open(attachment_path, "rb") as attachment:
        part = MIMEApplication(attachment.read())
    part.add_header("Content-Disposition", "attachment", filename=f"{report_name}.zip")
    msg.attach(part)
    raw_message = {"Data": msg.as_string()}

    # Step#3: Send email
    logger.info(f'Send email to {config.config[report_name]["report_recipient"]}')
    ses = boto3.client("ses")
    ses_response = ses.send_raw_email(
        RawMessage=raw_message,
        Source=msg["From"],
        Destinations=config.config[report_name]["report_recipient"],
    )
    logger.info(f'SES response ID received: {ses_response["MessageId"]}.')


def lambda_handler(event, context):
    """[summary]

    :param event: [description]
    :type event: [type]
    :param context: [description]
    :type context: [type]
    :return: [description]
    :rtype: [type]
    """

    begin = time.time()
    report_name = "monthly_risk_report"
    build_and_send_report(report_name)
    end = time.time()
    logger.info(
        f"Total minutes taken for this Lambda to run: {float((end - begin)/60):.2f}"
    )
    logger.info("Done")
