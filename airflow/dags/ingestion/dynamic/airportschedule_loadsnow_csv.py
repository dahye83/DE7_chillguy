from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.models import Variable
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import logging
import traceback 
import requests

#slack 설정
SLACK_WEBHOOK_URL = Variable.get("slack_webhook_url")

def slack_on_failure(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    exception = context.get("exception")

    error_msg = "".join(
        traceback.format_exception(None, exception, exception.__traceback__)
    ) if exception else "No exception info"

    message = (
        f":red_circle: *DAG {dag_id}*의 Task *{task_id}* 실패 ❌\n"
        f"Execution Date: {execution_date}\n"
        f"Error:\n```{error_msg}```\n"
        f"<{log_url}|🔗 로그 보기>"
    )
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})

def slack_on_success(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")

    message = (
        f":white_check_mark: *DAG {dag_id}* Task *{task_id}* 성공 🎉\n"
        f"Execution Date: {execution_date}"
    )
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})


# S3 설정
BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_PATH = "airportschedule_loads3_csv"   # S3에 저장된 파일명 그대로 사용

default_args = {
    "owner": "sihyun",
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

@dag(
    dag_id="airport_schedule_to_snowflake",
    start_date=datetime(2025, 11, 17),
    schedule=None,          # 외부 DAG에서 Trigger
    catchup=False,
    default_args=default_args,
    tags=["snowflake"]
)
def dag_airport_schedule_loadsnow():

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
        @task(
            task_id=task_id,
            on_failure_callback=slack_on_failure,   
            on_success_callback=slack_on_success    
        )
        
        # 1️⃣ S3 CSV 가져오기
        def s3_to_snowflake():
            s3_hook = S3Hook(aws_conn_id="s3_conn_id")
            obj = s3_hook.get_key(s3_key, bucket_name=s3_bucket)
            df = pd.read_csv(BytesIO(obj.get()['Body'].read()), encoding='utf-8-sig')

            if df.empty:
                raise ValueError(f"S3 파일이 비어있습니다: s3://{s3_bucket}/{s3_key}")
            
            df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d").dt.date
            df["TIME"] = pd.to_datetime(df["TIME"], format="%H:%M").dt.time

            # 2️⃣ Snowflake Hook
            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            conn = hook.get_conn() # 실제 커넥션 객체 가져오기
            cs = conn.cursor()

            # 컬럼 타입 매핑
            sf_cols = []
            for col in df.columns:
                col_up = col.upper()
                if col_up == "DATE":
                    sf_cols.append(f'"{col}" DATE')
                elif col_up == "TIME":
                    sf_cols.append(f'"{col}" TIME')
                else:
                    sf_cols.append(f'"{col}" VARCHAR')

            create_cols = ", ".join(sf_cols)

            # 컬럼 생성
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
                    {create_cols}
                )
            """
            cs.execute(create_sql)

            # 4️⃣ pandas → Snowflake 적재
            success, nchunks, nrows, _ = write_pandas(
                conn, df, table_name=table_name, schema=schema, database=database
            )

            if success:
                logging.info(f"Snowflake 적재 완료! 총 {nrows} rows")
            else:
                logging.error("❌ Snowflake 적재 실패")

            cs.close()
            conn.close()

        return s3_to_snowflake()

    # =========================
    # 4️⃣ DAG Task
    # =========================
    snowflake_task = create_s3_to_snowflake_task(
        task_id="s3_to_snowflake",
        s3_bucket=BUCKET_NAME,
        s3_key=S3_PATH,
        snowflake_conn_id="snowflake_conn_id",
        database="TEAM_7_CHILL",
        schema="BRONZE",
        table_name="BR_AIRPORT_SCHEDULE_7_DAILY"
    )

dag_airport_schedule_loadsnow()
