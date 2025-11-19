from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
import logging

BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_PATH = "airportschedule_loads3_csv"  


# =========================
# 3️⃣ 범용 S3 → Snowflake 적재 Task 생성 함수
# =========================
def create_s3_to_snowflake_task(
    task_id: str,
    s3_bucket: str,
    s3_key: str,
    snowflake_conn_id: str,
    database: str,
    schema: str,
    table_name: str,
):

    @task(task_id=task_id)
    def s3_to_snowflake():

        # 1️⃣ S3 CSV 가져오기
        s3_hook = S3Hook(aws_conn_id="s3_conn_id")
        obj = s3_hook.get_key(s3_key, bucket_name=s3_bucket)
        df = pd.read_csv(BytesIO(obj.get()['Body'].read()), encoding='utf-8-sig')

        if df.empty:
            raise ValueError(f"S3 파일 비어 있음: s3://{s3_bucket}/{s3_key}")

        # 2️⃣ Snowflake 연결
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        conn = hook.get_conn()
        cs = conn.cursor()

        # =========================
        # 컬럼 타입 자동 생성 (DATE, TIME만 특수 처리)
        # =========================
        sf_columns = []
        for col in df.columns:
            col_upper = col.upper()

            if col_upper == "DATE":
                sf_columns.append(f'"{col}" DATE')
            elif col_upper == "TIME":
                sf_columns.append(f'"{col}" TIME')
            else:
                sf_columns.append(f'"{col}" VARCHAR')

        create_cols = ", ".join(sf_columns)

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
                {create_cols}
            );
        """

        try:
            cs.execute(create_sql)

            # 4️⃣ pandas → Snowflake 적재
            success, nchunks, nrows, _ = write_pandas(
                    conn, df, table_name, schema=schema, database=database
                )

            if success:
                logging.info(f"Snowflake 적재 완료 ✅ Total rows: {nrows}")
            else:
                logging.error("Snowflake 적재 실패 ❌")

        finally:
            cs.close()
            conn.close()

    return s3_to_snowflake()


# =========================
# 4️⃣ DAG 정의
# =========================
@dag(
    dag_id="airportschedule_loadsnow_csv",
    schedule="10 5 * * *",
    start_date=datetime(2025, 11, 17),
    catchup=False,
    tags=["snowflake", "airport"],
)
def dag_airport_schedule_loadsnow():

    # 기존 DAG A(airportschedule_loads3_csv)에서 정의한 task 호출 가능
    fetch_task = fetch_7days()
    upload_task = upload_to_s3(fetch_task)

    snowflake_task = create_s3_to_snowflake_task(
        task_id="s3_to_snowflake",
        s3_bucket=BUCKET_NAME,
        s3_key=S3_PATH,
        snowflake_conn_id="snowflake_conn_id",
        database="TEAM_7_CHILL",
        schema="BRONZE",
        table_name="BR_AIRPORT_SCHEDULE_7_DAILY",
    )

    upload_task >> snowflake_task


dag_airport_schedule_loadsnow()
