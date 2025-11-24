from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import requests

# 기본 설정
default_args = {
    "owner": "keeyong",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

SNOWFLAKE_CONN_ID = "snowflake_conn_id"
DATABASE = "TEAM_7_CHILL"
SCHEMA = "GOLD"

# Preset 인증 정보 (Airflow Variable에서 가져오기)
PRESET_USER = Variable.get("PRESET_API_USER")
PRESET_PASSWORD = Variable.get("PRESET_API_PASSWORD")
PRESET_BASE_URL = "https://ede93244.us1a.app.preset.io"  # Preset 호스트
DASHBOARD_ID = "8"  # 갱신할 Dashboard ID

with DAG(
    dag_id="gold_airport_passengers_build",
    start_date=datetime(2025, 11, 15),
    schedule=None,  # Trigger 전용
    catchup=False,
    default_args=default_args,
    tags=["passengers", "snowflake", "gold"],
) as dag:

    @task
    def create_gold_passenger_summary():
        """GOLD 테이블 생성 및 데이터 적재"""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        sql = f"""
        -- 가장 최근 날짜만 선택
        WITH MAX_DATE AS (
            SELECT MAX("DATE") AS MAX_DATE
            FROM TEAM_7_CHILL.SILVER.SL_DIM_TIME
        ),
        ENRICHED AS (
            SELECT
                t."DATE",
                t."TIME",
                -- TIME_BLOCK 생성 (예: 00_01)
                LPAD(SPLIT_PART(t."TIME", '_', 1), 2, '0') || '_' ||
                LPAD(SPLIT_PART(t."TIME", '_', 2), 2, '0') AS TIME_BLOCK,
                CAST(SPLIT_PART(t."TIME", '_', 1) AS INT) AS START_HOUR,
                g.TERMINAL,
                g.GATE_TY,
                f.PASSENGER_CNT
            FROM TEAM_7_CHILL.SILVER.SL_FACT_PASSENGER f
            JOIN TEAM_7_CHILL.SILVER.SL_DIM_TIME t
              ON f.TIME_ID = t.TIME_ID
            JOIN TEAM_7_CHILL.SILVER.SL_DIM_GATE g
              ON f.GATE_ID = g.GATE_ID
            CROSS JOIN MAX_DATE md
            WHERE t."DATE" = md.MAX_DATE
        )
        SELECT
            "DATE",
            TIME_BLOCK,
            TERMINAL,
            GATE_TY,
            SUM(PASSENGER_CNT) AS TOTAL_PASSENGERS
        FROM ENRICHED
        GROUP BY "DATE", TIME_BLOCK, START_HOUR, TERMINAL, GATE_TY
        ORDER BY START_HOUR, TERMINAL, GATE_TY;
        """
        hook.run(sql)
        logging.info("GD_PASSENGER_SUMMARY 생성 완료")

    def refresh_preset_dashboard():
        """Preset Dashboard 자동 갱신"""
        url = f"{PRESET_BASE_URL}/api/v1/dashboard/{DASHBOARD_ID}/refresh"
        try:
            response = requests.post(url, auth=(PRESET_USER, PRESET_PASSWORD))
            if response.status_code == 200:
                logging.info("Preset Dashboard 갱신 성공")
            else:
                logging.error(f"Preset Dashboard 갱신 실패: {response.status_code} {response.text}")
        except Exception as e:
            logging.error(f"Preset API 호출 중 오류 발생: {e}")
            raise

    refresh_dashboard_task = PythonOperator(
        task_id="refresh_preset_dashboard",
        python_callable=refresh_preset_dashboard,
    )

    # DAG 순서: GOLD 테이블 생성 → Preset Dashboard 갱신
    create_gold_passenger_summary() >> refresh_dashboard_task
