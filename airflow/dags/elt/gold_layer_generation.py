from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

from SlackAlert import send_slack_success_callback, send_slack_failure_callback


# SQL 폴더 및 파일 정의
SQL_FOLDER = Path("/opt/airflow/sql/gold")
SQL_FILES = [
    "GD_AIRLINE_DELAY_INFO.SQL",
    "GD_AIRLINE_DELAY_REASON.SQL",
    "GD_AIRLINE_FLIGHT_CNT.SQL",
    "GD_DELAY_HOURLY.SQL",
    "GD_DELAY_REASON_CNT.SQL",
    "GD_DELAY_WEEKDAY.SQL",
    "GD_NATION_AIRLINE_SHARE.SQL",
    "GD_NATION_FLIGHT_CNT.SQL",
    "GD_ROUTE_AIRLINE_SHARE.SQL",
    "GD_FLIGHT_COUNT_7DAYS.SQL",
]

# Utility: SnowflakeOperator Task 생성
def create_snowflake_task(task_id: str, sql_path: Path, database: str, schema: str):
    if not sql_path.exists():
        raise FileNotFoundError(f"❌ SQL 파일이 존재하지 않습니다: {sql_path}")

    sql_content = sql_path.read_text()
    print(f"📄 SQL FILE PATH: {sql_path}")
    print(f"📝 SQL CONTENT:\n{sql_content}")

    return SnowflakeOperator(
        task_id=task_id,
        snowflake_conn_id="snowflake_conn_id",
        sql=sql_content,
        database=database,
        schema=schema
    )

# DAG 정의
default_args = {
    'owner': 'dahye',
    "depends_on_past": False,
    "retries": 1,     
    "retry_delay": timedelta(seconds=60),
}

with DAG(
    dag_id="gold_layer_generation",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    on_success_callback=send_slack_success_callback,
    on_failure_callback=send_slack_failure_callback,
) as dag:

    # SQL Task 생성 및 순서 연결
    tasks = []
    for idx, sql_file in enumerate(SQL_FILES, start=1):
        sql_path = SQL_FOLDER / sql_file
        task = create_snowflake_task(
            task_id=f"run_sql_{idx}",
            sql_path=sql_path,
            database="TEAM_7_CHILL",
            schema="GOLD"
        )
        tasks.append(task)

    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
