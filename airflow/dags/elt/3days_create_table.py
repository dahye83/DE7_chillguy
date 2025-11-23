from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow import DAG
from SlackAlert import send_slack_success_callback, send_slack_failure_callback

from datetime import datetime, timedelta

# 실버 테이블 생성
# TEMP 테이블 생성 후 두 테이블을 세로로 연결
# 연결 후 세계 공항 정보와 조인하여 필요한 컬럼 추가
sql_sliver = """
CREATE OR REPLACE TEMP TABLE SILVER.PASSENGER_FLIGHT_TEMP AS
SELECT
    AIRLINE, IATA_CD, FLIGHT_CD, TERMINAL, SCHEDULED_TM, ESTIMATED_TM, STATUS, FLIGHT_TYPE
FROM BRONZE.BR_3DAYS_ARRIVALS_DAILY
UNION ALL
SELECT
    AIRLINE, IATA_CD, FLIGHT_CD, TERMINAL, SCHEDULED_TM, ESTIMATED_TM, STATUS, FLIGHT_TYPE
FROM BRONZE.BR_3DAYS_DEPARTURE_DAILY;

CREATE OR REPLACE TABLE SILVER.SL_3DAYS_DAILY_MERGED AS
SELECT
    t.*,
    AIRPORT_NAME_KOR, NATION_NAME_KOR, REGION, CITY_KOR
FROM SILVER.PASSENGER_FLIGHT_TEMP t
JOIN BRONZE.BR_WORLD_AIRPORTS_INFO_STATIC a ON t.IATA_CD = a.IATA_CODE
"""

# 골드 테이블1: 최근 3일간 시간대별 출발, 도착 비행기 수
# 출발, 도착 비행기가 존재하지 않는 시간대가 있기 때문에 모든 시간대, 모든 타입(출발, 도착)에 대해 생성.
# 이후 타입, 시간대로 묶어 비행 개수 카운트
sql_gold_hour_status_count = """
CREATE OR REPLACE TABLE GOLD.GD_3DAYS_HOURLY_COUNT_DAILY AS
WITH HOUR AS (
    SELECT SEQ4() AS TIME_SLOT
    FROM TABLE(GENERATOR(ROWCOUNT => 24))
),
STATUS AS (
    SELECT COLUMN1 AS STATUS
    FROM VALUES ('도착'), ('출발')
),
RAW AS (
    SELECT
        HOUR(TO_TIMESTAMP(SCHEDULED_TM)) AS TIME_SLOT,
        STATUS,
        COUNT(*) AS CNT
    FROM SILVER.SL_3DAYS_DAILY_MERGED
    GROUP BY 1, 2
)
SELECT
    h.TIME_SLOT,
    s.STATUS,
    COALESCE(r.CNT, 0) AS CNT
FROM HOUR h
CROSS JOIN STATUS s
LEFT JOIN RAW r ON h.TIME_SLOT = r.TIME_SLOT AND s.STATUS = r.STATUS
ORDER BY h.TIME_SLOT, s.STATUS;
"""

# 골드 테이블2: 최근 3일간 인천공항에서 출발한 비행기가 방문한 나라별 횟수
sql_gold_nation_flight_count = """
CREATE OR REPLACE TABLE GOLD.GD_3DAYS_NATION_COUNT_DAILY AS
SELECT
    NATION_NAME_KOR,
    COUNT(*) AS FLIGHT_CNT
FROM SILVER.SL_3DAYS_DAILY_MERGED
WHERE STATUS = '출발'
GROUP BY 1
ORDER BY 1;
"""

# 골드 테이블3: 최근 3일간 인천공항에서 출발한 비행기가 방문한 지역 횟수
sql_gold_city_flight_count = """
CREATE OR REPLACE TABLE GOLD.GD_3DAYS_CITY_COUNT_DAILY AS
SELECT
    CITY_KOR,
    COUNT(*) AS FLIGHT_CNT
FROM SILVER.SL_3DAYS_DAILY_MERGED
WHERE STATUS = '출발'
GROUP BY 1
ORDER BY 1;
"""

with DAG(
    dag_id = '3dyas_create_sl_gd_table',
    start_date = datetime(2025, 11, 20),
    catchup = False,
    default_args = {
        'retries': 3,
        'retry_delay' : timedelta(minutes = 3),
        'on_failure_callback': send_slack_failure_callback
    },
    on_success_callback = send_slack_success_callback,
) as dag:
    
    create_silver_table_task = SnowflakeOperator(
        task_id = 'create_silver_table_task',
        snowflake_conn_id = 'snowflake_conn_id',
        sql = sql_sliver
    )

    create_gold_3days_houly_count_table_task = SnowflakeOperator(
        task_id = 'create_gold_3days_houly_count_table_task',
        snowflake_conn_id = 'snowflake_conn_id',
        sql = sql_gold_hour_status_count
    )

    # 사용되지 않는 태스크 주석처리
    #create_gold_3days_nation_count_table_task = SnowflakeOperator(
    #    task_id = 'create_gold_3days_nation_count_table_task',
    #    snowflake_conn_id = 'snowflake_conn_id',
    #    sql = sql_gold_nation_flight_count
    #)

    #create_gold_3days_city_count_table_task = SnowflakeOperator(
    #    task_id = 'create_gold_3days_city_count_table_task',
    #    snowflake_conn_id = 'snowflake_conn_id',
    #    sql = sql_gold_city_flight_count
    #)        

    create_silver_table_task >> create_gold_3days_houly_count_table_task# >> create_gold_3days_nation_count_table_task >> create_gold_3days_city_count_table_task


