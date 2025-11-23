from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    "owner": "team_7_chill",
    "retries": 2,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="gold_map_airport_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 5 * * *",  # 14:30 임시
    catchup=False,
    default_args=default_args,
    tags=["gold", "airport_info", "map"]
):

    build_gold_map = SnowflakeOperator(
        task_id="build_gold_map_airline",
        snowflake_conn_id="snowflake_conn_id",
        sql="""
            /* 7일 스케줄 기반 지도 시각화 + 항공사별 노선수 추가 */
            CREATE OR REPLACE TABLE GOLD.GD_MAP_AIRPORT AS
            WITH mapped AS (
                SELECT
                    /* 기본 정보 */
                    DEPARTURE_DT,
                    AIRLINE,
                    FLIGHT_CD,
                    DESTINATION,
                    IATA_CD,

                    /* 출발지 좌표 – 인천공항(ICN) 고정 */
                    '인천 공항' AS SRC_AIRPORT_NM,
                    'ICN' AS SRC_IATA_CD,
                    37.4602 AS SRC_LATITUDE,
                    126.4407 AS SRC_LONGITUDE,

                    /* 목적지 좌표 (SILVER에서 매핑된 값) */
                    LATITUDE AS DEST_LATITUDE,
                    LONGITUDE AS DEST_LONGITUDE,
                    NATION_NM_KOR AS DEST_NATION_KOR,

                    /* 항공사별 노선수: 고유 목적지 개수 */
                    COUNT(DISTINCT IATA_CD) OVER (PARTITION BY AIRLINE) AS ROUTE_COUNT

                FROM SILVER.SL_AIRPORT_INFO_7DAYS
                WHERE LATITUDE IS NOT NULL
                AND LONGITUDE IS NOT NULL
            )
            SELECT *
            FROM mapped;
        """
    )
    

    build_gold_map
