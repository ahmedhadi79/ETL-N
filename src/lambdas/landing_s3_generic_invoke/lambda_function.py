import json
import logging
import os
import time
import boto3
import botocore.config
import pandas as pd
from botocore.exceptions import ClientError
from flatten_json import flatten

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def set_pandas_display_options() -> None:
    """Set pandas display options."""
    display = pd.options.display
    display.max_columns = 1000
    display.max_rows = 1000
    display.max_colwidth = 199
    display.width = 1000


def parse_payload(event):
    """
    Accepts event
    :param event: Event from s3
    :return: A pandas dataframe
    """
    # final_df = pd.DataFrame()
    frames = []
    for record in event["Records"]:
        try:
            payload = record["body"]
            # Flatten JSON and convert to pandas
            logger.info("Flattening JSON...")
            json_payload = json.loads(payload)
            flat_json = flatten(json_payload)
            logger.info("Success in flattening JSON!")
            frames.append(flat_json)
            # final_df = final_df.append(flat_json, ignore_index=True)
        except Exception as e:
            logger.error("Flat JSON:  %s", flat_json)
            logger.error("Exception occurred:  %s", e)
            return e

    final_df = pd.DataFrame(frames)
    # fix nulls with empty string
    final_df = final_df.fillna("")
    return final_df


def lambda_arn(context, function):
    aws_account_id = context.invoked_function_arn.split(":")[4]
    region = context.invoked_function_arn.split(":")[3]
    arn = "arn:aws:lambda:{}:{}:{}".format(region, aws_account_id, function)
    return arn


def execute_lambda(event, context, function_name):
    cfg = botocore.config.Config(
        retries={"max_attempts": 0}, connect_timeout=900, read_timeout=900
    )
    client = boto3.client("lambda", config=cfg)
    arn = lambda_arn(context, function_name)
    print("Calling the lambda function for Reporting :" + str(arn))
    response = client.invoke(
        FunctionName=arn, InvocationType="RequestResponse", Payload=json.dumps(event)
    )
    print("Completed the lambda function for Report :" + str(arn))
    print("Response from lambda function : " + str(response))
    return response


def execute_glue(job_name, bucket_name, object_key):
    glue_client = boto3.client("glue")
    job_args = {"--bucket_name": bucket_name, "--object_key": object_key}
    try:
        response = glue_client.start_job_run(JobName=job_name, Arguments=job_args)
        print(f"Started Glue job: {job_name} with run ID: {response['JobRunId']}")
    except Exception as e:
        logger.error(f"Error starting Glue job: {str(e)}")
        raise e


def archive_objects_from_s3(s3_files_path):
    if "s3://" not in s3_files_path:
        raise Exception("Given path is not a valid s3 path.")
    s3_resource = boto3.client("s3")
    s3_tokens = s3_files_path.split("/")
    bucket_name = s3_tokens[2]
    folder = s3_tokens[3]
    object_path = ""
    result = ""
    filename = s3_tokens[len(s3_tokens) - 1]
    logger.info(f"bucket_name: {bucket_name}")
    logger.info(f"filename: {filename}")
    if len(s3_tokens) > 4:
        for tokn in range(3, len(s3_tokens) - 1):
            object_path += s3_tokens[tokn] + "/"
        object_path += filename
    else:
        object_path += filename
    logger.info(f"object: {object_path}")
    try:
        copy_source = {"Bucket": bucket_name, "Key": object_path.strip()}
        filename_ext = object_path.split("/")[-1]
        target_file_name = f"archived/{folder}/{filename_ext}"
        s3_resource.copy(copy_source, bucket_name, target_file_name)
        result = s3_resource.delete_object(Bucket=bucket_name, Key=object_path.strip())
    except ClientError as e:
        logger.error("Exception occurred:  %s", e)
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
    return result


def trigger_name(event_fldr):
    ename = ""
    if event_fldr == "imal_reporting":
        ename = os.environ["GLUE_NAME_IMAL_REPORT"]
    elif event_fldr == "paymentology_reporting":
        # TO DO - paymentology_reporting
        ename = os.environ["FUNCTION_NAME_PAYMENTTOLOGY_REPORT"]
    else:
        # TO DO - amplitude_reporting
        ename = os.environ["FUNCTION_NAME_AMPLITUDE_REPORT"]
    return ename


def lambda_handler(event, context):
    begin = time.time()
    set_pandas_display_options()
    final_df = parse_payload(event)
    bucket_name = final_df.loc[0]["Records_0_s3_bucket_name"]
    key_val = final_df.loc[0]["Records_0_s3_object_key"]
    path = f"s3://{bucket_name}/{key_val}"
    logger.info(f"s3-path: {path}")
    event_folder = key_val.split("/")[0]
    job_name = trigger_name(event_folder)
    execute_glue(job_name, bucket_name, key_val)

    end = time.time()
    logger.info(
        f"Total minutes taken for this Lambda to run: {float((end - begin)/60):.2f}"
    )
    logger.info("Done")
