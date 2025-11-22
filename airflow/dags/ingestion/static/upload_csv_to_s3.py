import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from SlackAlert import send_slack_failure_callback, send_slack_success_callback

# Configurations
BUCKET_NAME = Variable.get("BUCKET_NAME")
CSV_PATH = "/opt/airflow/data/preprocessed/airport_preprocessed.csv"
S3_KEY = "static/final_airport.csv"

# Utility Function
def validate_file(path):
    """주어진 파일이 존재하는지 확인"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV 파일이 존재하지 않습니다: {path}")

# ============================================================
# task : Upload CSV file to S3
# ============================================================
def upload_csv_to_s3():
    """
    - 1. CSV 존재 여부 확인
    - 2. s3 hook을 사용하여 S3 업로드
    - 3. 동일 파일이 존재하면 replace=True 로 덮어쓰기
    """

    validate_file(CSV_PATH)
    s3 = S3Hook(aws_conn_id="s3_conn_id")

    s3.load_file(
        filename=CSV_PATH,
        key=S3_KEY,
        bucket_name=BUCKET_NAME,
        replace=True
    )

# ============================================================
# DAG Definition
# ============================================================
default_args = {
    'owner': 'dahye',
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
}

with DAG(
    dag_id="upload_csv_to_s3",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task_upload_to_s3 = PythonOperator(
        task_id="upload_csv_to_s3",
        python_callable=upload_csv_to_s3,
        on_failure_callback=send_slack_failure_callback,
        on_success_callback=send_slack_success_callback,
    )
