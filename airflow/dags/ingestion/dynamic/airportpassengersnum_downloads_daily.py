import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO, BytesIO
import logging
import traceback
import boto3

# -----------------------------
# Slack Webhook 설정 (Airflow Variable)
# -----------------------------
SLACK_WEBHOOK_URL = Variable.get("slack_alert_url")

# -----------------------------
# S3 설정
# -----------------------------
BUCKET_NAME = "chillguy-bucket-seoul"
S3_PATH = "dynamic/airportpassengersnum_downloads_daily.csv"

def get_s3_client():
    hook = S3Hook(aws_conn_id="s3_conn_id")
    return hook.get_conn()

# -----------------------------
# Slack 알림 함수
# -----------------------------
def slack_on_failure(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    exception = context.get("exception")
    error_msg = "".join(traceback.format_exception(None, exception, exception.__traceback__)) if exception else "No exception info"

    message = (
        f":red_circle: DAG *{dag_id}* Task *{task_id}* 실패\n"
        f"Execution Date: {execution_date}\n"
        f"Error:\n```{error_msg}```\n"
        f"Log: {log_url}"
    )
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})

def slack_on_success(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    message = f":white_check_mark: DAG *{dag_id}* Task *{task_id}* 실행 성공 ✅\nExecution Date: {execution_date}"
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})

# -----------------------------
# DAG 기본 args
# -----------------------------
default_args = {
    "owner": "keeyong",
    "retries": 3,
    "retry_delay": timedelta(minutes=3)
}

# -----------------------------
# DAG 정의
# -----------------------------
with DAG(
    dag_id="airportpassengersnum_downloads_daily",
    start_date=datetime(2025, 11, 15),
    schedule="0 5 * * *",  # UTC 기준 5시 → 한국 시간 14시
    catchup=False,
    default_args=default_args,
    tags=["passengers", "s3", "slack", "snowflake"],
) as dag:

    # =========================
    # 1️⃣ 공공 API 데이터 수집 Task
    # =========================
    @task(
        retries=3,
        retry_delay=timedelta(minutes=3),
        on_failure_callback=slack_on_failure,
        on_success_callback=slack_on_success
    )
    def fetch_api_data():
        url = "https://apis.data.go.kr/B551177/passgrAnncmt/getPassgrAnncmt"
        service_key = Variable.get("serviceKey")
        all_items = []
        num_of_rows = 100

        for day_offset in range(0, 2):  # 2일치 조회
            page = 1
            logging.info(f"=== selectdate={day_offset} 조회 시작 ===")
            while True:
                params = {
                    "serviceKey": service_key,
                    "selectdate": str(day_offset),
                    "type": "json",
                    "numOfRows": str(num_of_rows),
                    "pageNo": str(page)
                }
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                items_container = data.get('response', {}).get('body', {}).get('items', None)
                if isinstance(items_container, list):
                    items = items_container
                elif isinstance(items_container, dict) and 'item' in items_container:
                    items = items_container['item']
                else:
                    items = []

                if not items:
                    logging.info(f"selectdate={day_offset}, page={page} 데이터 없음 → 종료")
                    break
                all_items.extend(items)

                if len(items) < num_of_rows:
                    break
                else:
                    page += 1

        logging.info(f"총 수집 데이터 건수: {len(all_items)}")
        return all_items

    # =========================
    # 2️⃣ S3 업로드 Task
    # =========================
    @task(
        retries=3,
        retry_delay=timedelta(minutes=3),
        on_failure_callback=slack_on_failure,
        on_success_callback=slack_on_success
    )
    def upload_to_s3(data):
        client = get_s3_client()
        if not data:
            logging.warning("수집된 데이터 없음. 종료")
            return
        df = pd.DataFrame(data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        csv_buffer.seek(0)

        try:
            client.put_object(
                Bucket=BUCKET_NAME,
                Key=S3_PATH,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv"
            )
            logging.info(f"S3 업로드 완료: s3://{BUCKET_NAME}/{S3_PATH}")
            logging.info(f"총 업로드 데이터 건수: {len(df)}")
        except Exception as e:
            logging.error(f"S3 업로드 실패: {e}")
            raise
        
        


    # =========================
    # 3️⃣ 범용 S3 → Snowflake 적재 Task 생성 함수 333
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
            task_id=task_id
        )
        def s3_to_snowflake():
            # 1️⃣ S3 CSV 가져오기
            s3_hook = S3Hook(aws_conn_id="s3_conn_id")
            obj = s3_hook.get_key(s3_key, bucket_name=s3_bucket)
            df = pd.read_csv(BytesIO(obj.get()['Body'].read()), encoding='utf-8-sig')

            if df.empty:
                raise ValueError(f"S3 파일이 비어있습니다: s3://{s3_bucket}/{s3_key}")

            # 2️⃣ Snowflake Hook
            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            conn = hook.get_conn()  # 실제 커넥션 객체 가져오기
            cs = conn.cursor()

            try:
                # 3️⃣ 테이블 생성
                create_cols = ", ".join([f'"{c}" STRING' for c in df.columns])
                cs.execute(f"""
                    CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} ({create_cols})
                """)

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
    # 4️⃣ DAG Task 연결
    # =========================
    data = fetch_api_data()
    upload_task = upload_to_s3(data)
    
    snowflake_task = create_s3_to_snowflake_task(
        task_id="s3_to_snowflake",
        s3_bucket=BUCKET_NAME,
        s3_key=S3_PATH,
        snowflake_conn_id="snowflake_conn_id",
        database="TEAM_7_CHILL",
        schema="BRONZE",
        table_name="AIRPORT_PASSENGERS_DAILY",
    )

    upload_task >> snowflake_task
