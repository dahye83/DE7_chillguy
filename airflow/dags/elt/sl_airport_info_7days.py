from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from SlackAlert import send_slack_success_callback, send_slack_failure_callback

default_args = {
    "owner": "team_7_chill",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": send_slack_failure_callback
}

with DAG(
    dag_id="silver_airport_info_7_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 5 * * *",  # 매일 14:30(임시)
    catchup=False,
    default_args=default_args,
    tags=["silver", "airport_info", "static"],
    on_success_callback= send_slack_success_callback
):

    transform_silver = SnowflakeOperator(
        task_id="build_sliver_airport_info_7",
        snowflake_conn_id="snowflake_conn_id",
        sql="""
            CREATE OR REPLACE TABLE SILVER.SL_AIRPORT_INFO_7DAYS AS
            WITH week7 AS (
                SELECT
                    s.DATE AS DEPARTURE_DT,
                    s.TIME AS DEPARTURE_TM,
                    s.AIRLINE,
                    s.FLIGHT AS FLIGHT_CD,
                    s.AIRPORT AS DESTINATION,
                    s.IATA_CODE AS IATA_CD,

                    w.AIRPORT_NAME_ENG AS AIRPORT_NM_ENG,
                    w.AIRPORT_NAME_KOR AS AIRPORT_NM_KOR,
                    w.NATION_NAME_ENG AS NATION_NM_ENG,
                    w.NATION_NAME_KOR AS NATION_NM_KOR,
                    w.CITY_ENG,
                    w.CITY_KOR,
                    w.LATITUDE,
                    w.LONGITUDE

                FROM BRONZE.BR_AIRPORT_SCHEDULE_7_DAILY s
                LEFT JOIN BRONZE.BR_WORLD_AIRPORTS_INFO_STATIC w
                    ON upper(s.IATA_CODE) = upper(w.IATA_CODE)
            ), 

            dedup AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            DEPARTURE_DT,
                            DEPARTURE_TM,
                            AIRLINE,
                            FLIGHT_CD,
                            IATA_CD
                        ORDER BY DEPARTURE_DT
                    ) AS rn
                FROM week7
            )

            SELECT 
                DEPARTURE_DT,
                DEPARTURE_TM,
                AIRLINE,
                FLIGHT_CD,
                DESTINATION,
                IATA_CD,
                AIRPORT_NM_ENG,
                AIRPORT_NM_KOR,
                NATION_NM_ENG,
                NATION_NM_KOR,
                CITY_ENG,
                CITY_KOR,
                LATITUDE,
                LONGITUDE
            FROM dedup
            WHERE rn = 1;
        """
    )

    transform_silver
