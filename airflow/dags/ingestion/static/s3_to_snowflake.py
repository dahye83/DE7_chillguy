import os
import logging
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

from SlackAlert import send_slack_failure_callback, send_slack_success_callback

# settings
BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_KEY = "static/final_airport.csv"


# Utility: Pandas dtype → Snowflake dtype 매핑
def map_dtype_to_snowflake(dtype) -> str:
    """pandas dtype을 Snowflake 컬럼 타입으로 변환하는 함수"""
    if pd.api.types.is_integer_dtype(dtype):
        return "NUMBER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP_NTZ"
    else:
        return "STRING"


# ============================================================
# Task Factory: create_csv_to_snowflake_task()
# S3에서 CSV를 읽어 Snowflake에 적재하는 Task를 생성
# ============================================================
def create_csv_to_snowflake_task(
    task_id: str,
    s3_bucket: str,
    s3_key: str,
    snowflake_conn_id: str,
    database: str,
    schema: str,
    table_name: str,
):
    @task(task_id=task_id)
    def s3_csv_to_snowflake():
        """
        - 1. S3에서 CSV 파일 다운로드
        - 2. CSV의 컬럼 타입 기반으로 Snowflake 테이블 생성 또는 교체
        - 3. CSV 데이터를 Snowflake로 적재
        """

        # 1. S3에서 CSV 파일 다운로드
        s3_hook = S3Hook(aws_conn_id="s3_conn_id")
        obj = s3_hook.get_key(s3_key, bucket_name=s3_bucket)

        df = pd.read_csv(BytesIO(obj.get()["Body"].read()), encoding="utf-8-sig")

        if df.empty:
            raise ValueError(f"S3 file is empty: s3://{s3_bucket}/{s3_key}")

        logging.info(f"S3에서 CSV 파일 로드 완료 — 총 {len(df)}행")

        # 2-1. Snowflake 연결
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        conn = hook.get_conn()
        cs = conn.cursor()

        try:
            # 2-2. 컬럼 및 테이블 생성
            column_defs = ", ".join(
                [f'"{col}" {map_dtype_to_snowflake(dtype)}' for col, dtype in df.dtypes.items()]
            )

            create_table_sql = f"""
            CREATE OR REPLACE TABLE {database}.{schema}.{table_name} (
                {column_defs}
            )
            """

            cs.execute(create_table_sql)
            logging.info(f"Snowflake 테이블 : {database}.{schema}.{table_name}")

            # 3. CSV 데이터를 Snowflake로 적재
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name,
                schema=schema,
                database=database
            )

            if success:
                logging.info(f"Snowflake 적재 완료 — 총 {nrows}행")
            else:
                raise RuntimeError("❌ Snowflake 데이터 적재 실패")

        finally:
            cs.close()
            conn.close()

    return s3_csv_to_snowflake()


# ============================================================
# DAG 정의
# ============================================================
default_args = {
    'owner': 'dahye',
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
    "on_failure_callback": send_slack_failure_callback,
    "on_success_callback": send_slack_success_callback,
}

with DAG(
    dag_id="s3_to_snowflake",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    csv_to_snowflake_task = create_csv_to_snowflake_task(
        task_id="load_csv_to_snowflake",
        s3_bucket=BUCKET_NAME,
        s3_key=S3_KEY,
        snowflake_conn_id="snowflake_conn_id",
        database="TEAM_7_CHILL",
        schema="BRONZE",
        table_name="BR_AIRPORT_SCHEDULE_STATIC_YEARLY",
    )

    csv_to_snowflake_task
