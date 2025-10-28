import io
import logging
import os
import time
from datetime import datetime
from datetime import timedelta

import awswrangler as wr
import boto3
import numpy as np
import pandas as pd

import config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_sql(sql_path):
    logger.info("Reading sql file... ")
    with open(sql_path, "r") as sql_file:
        sql = sql_file.read()
    return sql


def run_query(query):
    client = boto3.client("athena")
    res = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": os.environ["DATABASE"]},
        ResultConfiguration={
            "OutputLocation": "s3://" + os.environ["S3_ATHENA"] + "/output/",
        },
    )
    logger.info("Execution ID: " + res["QueryExecutionId"])

    try:
        query_status = None
        while (
            query_status == "QUEUED"
            or query_status == "RUNNING"
            or query_status is None
        ):
            query_status = client.get_query_execution(
                QueryExecutionId=res["QueryExecutionId"]
            )["QueryExecution"]["Status"]["State"]
            logger.info(query_status)
            if query_status == "FAILED" or query_status == "CANCELLED":
                raise Exception(
                    'Athena query with the string "{}" failed or was cancelled'.format(
                        query
                    )
                )
            time.sleep(10)
        logger.info('Query "{}" finished.'.format(query))

        resource = boto3.resource("s3")
        res = (
            resource.Bucket(os.environ["S3_CURATED"])
            .Object(key=res["QueryExecutionId"] + ".csv")
            .get()
        )
        df = pd.read_csv(io.BytesIO(res["Body"].read()), encoding="utf8")

        return df

    except Exception as e:
        logger.info(e)


def circumstance_process(custI):
    custI["Circumstance"] = custI["Circumstance"].str.rstrip(" |")
    cc = custI[["dynamodb_new_image_customer_id_s", "Circumstance"]].drop_duplicates()
    cc["SelfEmployed_OR_BusinessOwner"] = np.where(
        cc["Circumstance"].astype(str).str.contains("SELF_EMPLOYED"),
        "Y",
        cc["Circumstance"],
    )
    cc["SelfEmployed_OR_BusinessOwner"] = np.where(
        cc["Circumstance"].astype(str).str.contains("BUSINESS_OWNER"),
        "Y",
        cc["SelfEmployed_OR_BusinessOwner"],
    )
    cc["SelfEmployed_OR_BusinessOwner"] = np.where(
        cc["SelfEmployed_OR_BusinessOwner"] != "Y",
        "N",
        cc["SelfEmployed_OR_BusinessOwner"],
    )
    cc["SelfEmployed_OR_BusinessOwner"] = np.where(
        cc["Circumstance"].isnull(), np.nan, cc["SelfEmployed_OR_BusinessOwner"]
    )

    notnacc = cc[
        (cc["SelfEmployed_OR_BusinessOwner"] == "Y")
        | (cc["SelfEmployed_OR_BusinessOwner"] == "N")
    ]
    isnaacc = cc[cc["SelfEmployed_OR_BusinessOwner"].isna()]

    fin = isnaacc[
        ~isnaacc["dynamodb_new_image_customer_id_s"].isin(
            notnacc["dynamodb_new_image_customer_id_s"]
        )
    ]

    cc = pd.concat([notnacc, fin])
    return cc


def individual_process(dataframe):
    dataframe["dynamodb_new_image_updated_at_n"] = pd.to_datetime(
        dataframe["dynamodb_new_image_updated_at_n"]
    )
    get_min_date = (
        dataframe.groupby(["user_id"])["dynamodb_new_image_updated_at_n"]
        .min()
        .reset_index()
    )
    get_min_date.rename(
        columns={"dynamodb_new_image_updated_at_n": "join_Date"}, inplace=True
    )

    rel_users = get_min_date[get_min_date["join_Date"] >= "2023-04-16"][
        "user_id"
    ].unique()

    df = dataframe[dataframe["user_id"].isin(rel_users)]
    df = df.merge(get_min_date, on=["user_id"], how="left")
    df["join_Date"] = df["join_Date"].dt.date

    df[
        "dynamodb_new_image_individual_m_individual_screening_m_result_m_pep_check_passed_bool"
    ].unique()
    df["PEP"] = np.where(
        df[
            "dynamodb_new_image_individual_m_individual_screening_m_result_m_pep_check_passed_bool"
        ]
        .astype(str)
        .str.contains("False"),
        "Y",
        df[
            "dynamodb_new_image_individual_m_individual_screening_m_result_m_pep_check_passed_bool"
        ],
    )
    df["PEP"] = np.where(
        df[
            "dynamodb_new_image_individual_m_individual_screening_m_result_m_pep_check_passed_bool"
        ]
        .astype(str)
        .str.contains("True"),
        "N",
        df["PEP"],
    )
    df["PEP"].unique()
    return df


def get_ar(df):
    df_lr = df[df["rn_last"] == 1]
    ar = []
    for i in df_lr["user_id"]:
        n_df = df_lr[df_lr["user_id"] == i]

        if (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PASSED"
            and n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_MANUAL_REVIEW"
        ):
            ar.append([i, "[Complete IDV, Awaiting Address Review]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PASSED"
            and n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_QR_SCAN"
        ):
            ar.append([i, "[Complete IDV, Awaiting QR Scan]"])

        elif n_df[
            "dynamodb_new_image_individual_m_identity_verification_m_status_s"
        ].values == "PASSED" and (
            n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_MANUAL_INPUT"
            or n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_DOCUMENT_UPLOAD"
        ):
            ar.append([i, "[Complete IDV, Don't Complete POA]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PASSED"
            and n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_PROVIDER_SELECTION"
        ):
            ar.append([i, "[Complete IDV, Don't Start POA]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PASSED"
            and n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "VERIFIED"
            and n_df["dynamodb_new_image_status_s"].values == "AWAITING_SUBMISSION"
        ):
            ar.append([i, "[Complete IDV, POA - Don't Submit]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "UNKNOWN"
            and n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_PROVIDER_SELECTION"
            and n_df["dynamodb_new_image_status_s"].values == "AWAITING_SUBMISSION"
        ):
            ar.append([i, "[Don't start IDV, POA]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "AWAITING_CUSTOMER_RETRY"
            and n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_MANUAL_REVIEW"
            and n_df["dynamodb_new_image_status_s"].values == "AWAITING_MANUAL_REVIEW"
        ):
            ar.append([i, "[IDV incomplete, Awaiting Address Review]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "AWAITING_CUSTOMER_RETRY"
            or n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "AWAITING_MANUAL_REVIEW"
            or n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PENDING"
        ) and (
            n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "VERIFIED"
        ):
            ar.append([i, "[IDV Incomplete, POA Complete]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "AWAITING_CUSTOMER_RETRY"
            or n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "AWAITING_MANUAL_REVIEW"
            or n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PENDING"
        ) and (
            n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_MANUAL_REVIEW"
            or n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_MANUAL_INPUT"
            or n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_DOCUMENT_UPLOAD"
        ):
            ar.append([i, "[IDV incomplete, POA incomplete]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "AWAITING_CUSTOMER_RETRY"
            or n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "AWAITING_MANUAL_REVIEW"
            or n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PENDING"
        ) and (
            n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_QR_SCAN"
        ):
            ar.append([i, "[IDV incomplete,Awaiting QR Scan]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "AWAITING_CUSTOMER_RETRY"
            or n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "AWAITING_MANUAL_REVIEW"
            or n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PENDING"
        ) and (
            n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_PROVIDER_SELECTION"
        ):
            ar.append([i, "[IDV incomplete. Don't Start POA]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "UNKNOWN"
        ) and (
            n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_QR_SCAN"
        ):
            ar.append([i, "[IDV Not Started, Awaiting QR Scan]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "UNKNOWN"
        ) and (
            n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_MANUAL_REVIEW"
            or n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_MANUAL_INPUT"
            or n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "AWAITING_DOCUMENT_UPLOAD"
        ):
            ar.append([i, "[IDV Not Started, POA Incomplete]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "UNKNOWN"
        ) and (
            n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "VERIFIED"
        ):
            ar.append([i, "[IDV Not Started, POA Complete]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PASSED"
            and n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "VERIFIED"
            and (
                n_df["dynamodb_new_image_status_s"].values == "AWAITING_APPROVAL"
                or n_df["dynamodb_new_image_status_s"].values
                == "AWAITING_ADDITIONAL_DOCUMENTS"
            )
        ):
            ar.append([i, "[Submit Application, Don't Submit EDD Docs]"])

        elif (
            n_df[
                "dynamodb_new_image_individual_m_identity_verification_m_status_s"
            ].values
            == "PASSED"
            and n_df["dynamodb_new_image_individual_m_address_m_status_s"].values
            == "VERIFIED"
            and n_df["dynamodb_new_image_status_s"].values == "AWAITING_MANUAL_REVIEW"
        ):
            ar.append([i, "[Submitted EDD Docs]"])

        else:
            ar.append([i, np.nan])

    bucket_final = pd.DataFrame(ar, columns=["user_id", "Bucket_"])
    df1 = df_lr.merge(bucket_final, on=["user_id"], how="left")
    return df1


def get_qr_final(df, df1):
    today = datetime.now()
    df_lr = df[df["rn_last"] == 1]
    waiting_qr = df_lr[
        df_lr["dynamodb_new_image_individual_m_address_m_status_s"]
        == "AWAITING_QR_SCAN"
    ]
    qr = df[df.user_id.isin(waiting_qr["user_id"])]
    qr_first = qr[
        (qr["rn_add_last"] == 1)
        & (
            qr["dynamodb_new_image_individual_m_address_m_status_s"]
            == "AWAITING_QR_SCAN"
        )
    ]

    def diff(start, end):
        x = pd.to_datetime(end) - pd.to_datetime(start)
        return int(x / np.timedelta64(1, "W"))

    qr_first["no_weeks"] = qr_first.apply(
        lambda row: diff(row["dynamodb_new_image_updated_at_n"], today), axis=1
    )

    qr_first = qr_first[qr_first["no_weeks"] >= 3]

    qr_first["Weeks_Waiting_for_QR_Scan"] = ["3 weeks or more"] * len(qr_first)

    qr_final = qr_first[["user_id", "Weeks_Waiting_for_QR_Scan"]]
    df2 = df1.merge(qr_final, on=["user_id"], how="left")
    return df2


def customer_reference(salesforce, df2, cc):
    sf = salesforce[["customer_reference_id__c"]].drop_duplicates()
    sf["customer_reference_id__c"] = sf["customer_reference_id__c"].astype("object")
    df3 = df2.merge(
        sf, left_on=["user_id"], right_on=["customer_reference_id__c"], how="left"
    )
    df3["Rework_YN"] = np.where(df3["customer_reference_id__c"].isnull(), " ", "Y")
    df3.drop(
        ["rn_last", "rn_first", "rn_add_last", "rn", "customer_reference_id__c"],
        axis=1,
        inplace=True,
    )
    df3 = df3.merge(
        cc[["dynamodb_new_image_customer_id_s", "SelfEmployed_OR_BusinessOwner"]],
        left_on=["user_id"],
        right_on=["dynamodb_new_image_customer_id_s"],
        how="left",
    )
    return df3


def get_partnership_report(df):
    yesterday = datetime.now() - timedelta(days=1)
    yday = yesterday.strftime("%d%m%y")

    filepath = (
        "s3://"
        + os.environ["S3_CURATED"]
        + "/partnership_reporting/"
        + "output_"
        + yday
        + "_EOD.csv"
    )
    o = wr.s3.read_csv(path=filepath)

    drop_list = [
        "Nickname",
        "PEP",
        "SelfEmployed_OR_BusinessOwner",
        "age_range",
        "Male_Female",
    ]
    o = o.drop([x for x in drop_list if x in o.columns], axis=1)
    new_customers = df[~df.user_id.isin(o["user_id"])]
    new_customers.rename(columns={"Bucket_": "Original_Bucket"}, inplace=True)
    new_customers = new_customers[
        (new_customers["dynamodb_new_image_status_s"] != "APPROVED")
        & (new_customers["dynamodb_new_image_status_s"] != "REJECTED")
    ]

    update = df[df.user_id.isin(o["user_id"])]

    o.rename(columns={"Bucket": "Original_Bucket"}, inplace=True)
    o["Original_Bucket"] = o["Original_Bucket"].str.replace("}", "]")

    o1 = o.merge(
        update[
            [
                "user_id",
                "Nickname",
                "age_range",
                "Male_Female",
                "Bucket_",
                "dynamodb_new_image_status_s",
                "Weeks_Waiting_for_QR_Scan",
                "Rework_YN",
                "dynamodb_new_image_individual_m_identity_verification_m_status_s",
                "dynamodb_new_image_individual_m_address_m_status_s",
                "PEP",
                "SelfEmployed_OR_BusinessOwner",
            ]
        ],
        on="user_id",
        how="left",
    )
    o1["Moved Y/N"] = np.where(o1["Original_Bucket"] != o1["Bucket_"], "Y", "N")
    o1["Onboarded Y/N"] = np.where(
        o1["dynamodb_new_image_status_s"] == "APPROVED", "Y", o1["Onboarded Y/N"]
    )

    o1["Rework Y/N"] = np.where(o1["Rework_YN"] == "Y", "Y", o1["Rework Y/N"])
    o1.drop(["Latest_Bucket"], axis=1, inplace=True)
    o1["Bucket_"] = np.where(
        o1["dynamodb_new_image_status_s"] == "APPROVED",
        o1["dynamodb_new_image_status_s"],
        o1["Bucket_"],
    )
    o1["Bucket_"] = np.where(
        o1["dynamodb_new_image_status_s"] == "REJECTED",
        o1["dynamodb_new_image_status_s"],
        o1["Bucket_"],
    )
    o1["Bucket_"] = np.where(
        o1["dynamodb_new_image_status_s"] == "AWAITING_EXIT_PERIOD",
        o1["dynamodb_new_image_status_s"],
        o1["Bucket_"],
    )
    o1["Bucket_"] = np.where(
        o1["dynamodb_new_image_status_s"] == "CEASED",
        o1["dynamodb_new_image_status_s"],
        o1["Bucket_"],
    )
    o1["Bucket_"] = np.where(
        o1["dynamodb_new_image_status_s"] == "CLOSED",
        o1["dynamodb_new_image_status_s"],
        o1["Bucket_"],
    )

    o1["Bucket_"] = np.where(
        o1["dynamodb_new_image_status_s"] == "AWAITING_BANK_ACCOUNT_CREATION",
        o1["dynamodb_new_image_status_s"],
        o1["Bucket_"],
    )

    o1.rename(columns={"Bucket_": "Latest_Bucket"}, inplace=True)

    o1["Weeks Waiting for QR Scan"] = np.where(
        o1["Weeks Waiting for QR Scan"] != o1["Weeks_Waiting_for_QR_Scan"],
        o1["Weeks_Waiting_for_QR_Scan"],
        o1["Weeks Waiting for QR Scan"],
    )

    o2 = o1[
        [
            "user_id",
            "First_Name",
            "Last_Name",
            "dynamodb_new_image_email_s",
            "dynamodb_new_image_phone_number_s",
            "age_range",
            "Male_Female",
            "Application Status",
            "IDV Status",
            "Address Status",
            "Brand ID",
            "Join Date",
            "Original_Bucket",
            "Weeks Waiting for QR Scan",
            "Rework Y/N",
            "PEP",
            "SelfEmployed_OR_BusinessOwner",
            "Called Y/N",
            "Onboarded Y/N",
            "Nickname",
            "Latest_Bucket",
            "Moved Y/N",
        ]
    ]

    o2["Onboarded Y/N"] = o2["Onboarded Y/N"].str.replace("nan", "N")

    o2.drop_duplicates(inplace=True)

    o1[o1["Latest_Bucket"].isnull()][
        [
            "user_id",
            "Original_Bucket",
            "dynamodb_new_image_individual_m_identity_verification_m_status_s",
            "dynamodb_new_image_individual_m_address_m_status_s",
            "dynamodb_new_image_status_s",
            "Latest_Bucket",
        ]
    ]

    new_customers.rename(
        columns={
            "dynamodb_new_image_status_s": "Application Status",
            "dynamodb_new_image_individual_m_identity_verification_m_status_s": "IDV Status",
            "dynamodb_new_image_individual_m_address_m_status_s": "Address Status",
            "join_Date": "Join Date",
            "dynamodb_new_image_brand_id_s": "Brand ID",
            "Weeks_Waiting_for_QR_Scan": "Weeks Waiting for QR Scan",
            "Rework_YN": "Rework Y/N",
        },
        inplace=True,
    )

    new_customers["Called Y/N"] = ["N"] * len(new_customers)
    new_customers["Onboarded Y/N"] = ["N"] * len(new_customers)
    new_customers["Moved Y/N"] = [""] * len(new_customers)
    new_customers["Latest_Bucket"] = [""] * len(new_customers)

    new_customers = new_customers[
        [
            "user_id",
            "First_Name",
            "Last_Name",
            "dynamodb_new_image_email_s",
            "dynamodb_new_image_phone_number_s",
            "age_range",
            "Male_Female",
            "Application Status",
            "IDV Status",
            "Address Status",
            "Brand ID",
            "Join Date",
            "Original_Bucket",
            "Weeks Waiting for QR Scan",
            "Rework Y/N",
            "PEP",
            "SelfEmployed_OR_BusinessOwner",
            "Called Y/N",
            "Onboarded Y/N",
            "Nickname",
            "Latest_Bucket",
            "Moved Y/N",
        ]
    ]

    final_df = pd.concat([o2, new_customers])

    return final_df


def lambda_handler(event, context):
    begin = time.time()

    dataframe = run_query(read_sql(config.config["queries"]["qq"]))
    salesforce = run_query(read_sql(config.config["queries"]["sfq"]))
    custI = run_query(read_sql(config.config["queries"]["qid"]))

    df = individual_process(dataframe)
    df1 = get_ar(df)
    df2 = get_qr_final(df, df1)
    cc = circumstance_process(custI)
    df3 = customer_reference(salesforce, df2, cc)
    final_df = get_partnership_report(df3)

    tod = datetime.now().strftime("%d%m%y")
    output_filename = "output_" + tod + "_EOD.csv"
    path = (
        "s3://" + os.environ["S3_CURATED"] + "/partnership_reporting/" + output_filename
    )
    wr.s3.to_csv(df=final_df, path=path)

    end = time.time()
    logger.info(
        f"Total minutes taken for this Lambda to run: {float((end - begin) / 60):.2f}"
    )
    return True
