import logging
import json
import ast
import traceback
from datetime import datetime
from typing import Literal, Optional
import boto3
import pandas as pd
import awswrangler as wr
from data_catalog import schemas


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)


# -------- Errors
class CustomError(Exception): ...


# -------- Secrets
def get_secret(secret_name: str) -> str:
    try:
        sm = boto3.client("secretsmanager")
        return sm.get_secret_value(SecretId=secret_name)["SecretString"]
    except Exception as e:
        logger.error(f"get_secret failed {secret_name}: {e}\n{traceback.format_exc()}")
        raise CustomError(f"Failed to fetch secret {secret_name}")


# -------- S3 helpers (used by mandates_delta)
class S3Utils:
    def __init__(self, client):
        self.client = client

    def list_parquet(self, bucket, prefix):
        try:
            resp = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [
                o["Key"]
                for o in resp.get("Contents", [])
                if o["Key"].endswith(".parquet")
            ]
        except Exception as e:
            logger.warning(f"list_parquet error {bucket}/{prefix}: {e}")
            return []

    def load_parquet(self, bucket, keys):
        dfs = []
        for k in keys:
            try:
                obj = self.client.get_object(Bucket=bucket, Key=k)
                dfs.append(pd.read_parquet(obj["Body"]))
            except Exception as e:
                logger.warning(f"read_parquet error {k}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# -------- Data shaping
def safe_convert_to_dict(v):
    if isinstance(v, str):
        try:
            return ast.literal_eval(v)
        except Exception:
            try:
                return json.loads(v)
            except Exception:
                return None
    return v


DICT_COLS = [
    "amount",
    "counterpartAccount",
    "ultimateRemitterAccount",
    "ultimateBeneficiaryAccount",
]


def flatten_dict_cols(df: pd.DataFrame) -> pd.DataFrame:
    for col in DICT_COLS:
        if col in df.columns:
            s = df[col].apply(safe_convert_to_dict)
            if s.notnull().any():
                flat = pd.json_normalize(s.dropna(), sep="_")
                flat.columns = [f"{col}_{c}" for c in flat.columns]
                df = df.drop(columns=[col]).join(flat)
    return df


def align_and_cast(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    schema = schemas.get(table_name) or {}
    cols = list(schema.keys())
    common = [c for c in cols if c in df.columns]
    df = df[common]
    for col, typ in schema.items():
        if col not in df.columns:
            continue
        try:
            if typ in ["date", "datetime"] or col in [
                "timestamp_extracted",
                "timestamp",
            ]:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif typ == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif typ == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
            elif typ == "string":
                df[col] = df[col].astype(str)
            else:
                df[col] = df[col].astype(typ)
        except Exception as e:
            logger.warning(f"cast fail {col}->{typ}: {e}")
    return df


def s3_write_raw(
    df: pd.DataFrame,
    target_table: str,
    env: str,
    mode: Literal["append", "overwrite", "overwrite_partitions"] = "append",
    no_partition: bool = False,
    rows_chunk: int = 400000,
    boto_session: Optional[boto3.session.Session] = None,
):
    if df.empty:
        logger.info(f"No rows to write for {target_table}")
        return
    bucket = f"bb2-{env}-datalake-raw"
    path = f"s3://{bucket}/{target_table}/"
    # ingestion meta
    utc_now = datetime.utcnow()
    df = df.copy()
    df["date"] = utc_now.date().strftime("%Y-%m-%d")
    df["timestamp_extracted"] = pd.to_datetime(
        utc_now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    )
    # align and cast to schema
    df = align_and_cast(df, target_table)
    logger.info(f"Writing {len(df)} rows to {path}")
    wr.s3.to_parquet(
        df=df,
        path=path,
        database="datalake_raw",
        table=target_table,
        partition_cols=None if no_partition else ["date"],
        mode=mode,
        max_rows_by_file=rows_chunk,
        use_threads=True,
        dataset=True,
        schema_evolution=True,
        compression="snappy",
        dtype=schemas.get(target_table),
        boto3_session=boto_session,
    )


# -------- ClearBank API helpers
def build_table_name(cb_table: str, cb_filter_object: Optional[str]) -> str:
    t = cb_table.lower()
    return (
        f"cb_{t}_temp"
        if not cb_filter_object
        else f"cb_{t}_{cb_filter_object.lower()}_temp"
    )
