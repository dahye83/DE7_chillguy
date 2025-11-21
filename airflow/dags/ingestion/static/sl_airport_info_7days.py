from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    "owner": "team_7_chill",
    "retries": 2,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="silver_airport_info_7_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 5 * * *",  # 매일 14:30(임시)
    catchup=False,
    default_args=default_args,
    tags=["silver", "airport_info", "static"]
):

    transform_silver = SnowflakeOperator(
        task_id="build_sliver_airport_info_7",
        snowflake_conn_id="snowflake_conn_id",
        sql="sql/sl_airport_info_7days.sql"   # 여기서 SQL 파일 실행
    )

    transform_silver
