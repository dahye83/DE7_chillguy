from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "keeyong",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="silver_airport_passengers_build",
    start_date=datetime(2025, 11, 15),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["passengers", "snowflake", "silver"],
) as dag:

    SNOWFLAKE_CONN_ID = "snowflake_conn_id"
    DATABASE = "TEAM_7_CHILL"
    SCHEMA = "SILVER"
    BRONZE_TABLE = f"{DATABASE}.BRONZE.BR_AIRPORT_PASSENGERS_DAILY"

    @task
    def create_dim_time():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        sql = f"""
        CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.SL_DIM_TIME (
            TIME_ID NUMBER AUTOINCREMENT PRIMARY KEY,
            "DATE" INT,
            "TIME" VARCHAR,
            START_TM TIME,
            END_TM TIME
        );

        INSERT INTO {DATABASE}.{SCHEMA}.SL_DIM_TIME ("DATE", "TIME")
        SELECT DISTINCT TRY_TO_NUMBER("adate") AS "DATE", "atime" AS "TIME"
        FROM {BRONZE_TABLE};
        """
        hook.run(sql)
        logging.info("SL_DIM_TIME 생성/적재 완료")

    @task
    def create_dim_gate():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        sql = f"""
        CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.SL_DIM_GATE (
            GATE_ID NUMBER AUTOINCREMENT PRIMARY KEY,
            TERMINAL VARCHAR,
            GATE_CD VARCHAR,
            GATE_TY VARCHAR,
            DIRECTION VARCHAR,
            DESCRIPTION VARCHAR
        );

        INSERT INTO {DATABASE}.{SCHEMA}.SL_DIM_GATE (TERMINAL, GATE_CD, DIRECTION)
        SELECT DISTINCT 'TBD', "tmp1", "tmp2"
        FROM {BRONZE_TABLE};
        """
        hook.run(sql)
        logging.info("SL_DIM_GATE 생성/적재 완료")


    @task
    def create_fact_passenger():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        sql = f"""
        CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.SL_FACT_PASSENGER (
            FACT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
            TIME_ID NUMBER,
            GATE_ID NUMBER,
            PASSENGER_CNT NUMBER
        );

        INSERT INTO {DATABASE}.{SCHEMA}.SL_FACT_PASSENGER (TIME_ID, GATE_ID, PASSENGER_CNT)
        SELECT
            t.TIME_ID,
            g.GATE_ID,
            COALESCE(
                NVL("t1eg1",0) + NVL("t1eg2",0) + NVL("t1eg3",0) + NVL("t1eg4",0) + NVL("t1egsum1",0) +
                NVL("t1dg1",0) + NVL("t1dg2",0) + NVL("t1dg3",0) + NVL("t1dg4",0) + NVL("t1dg5",0) + NVL("t1dg6",0) + NVL("t1dgsum1",0) +
                NVL("t2eg1",0) + NVL("t2eg2",0) + NVL("t2egsum1",0) +
                NVL("t2dg1",0) + NVL("t2dg2",0) + NVL("t2dgsum2",0)
            , 0) AS PASSENGER_CNT
        FROM {BRONZE_TABLE} b
        LEFT JOIN {DATABASE}.{SCHEMA}.SL_DIM_TIME t
            ON TRY_TO_NUMBER(b."adate") = t."DATE"
            AND b."atime" = t."TIME"
        LEFT JOIN {DATABASE}.{SCHEMA}.SL_DIM_GATE g
            ON b."tmp1" = g.GATE_CD AND b."tmp2" = g.DIRECTION
        ;
        """
        hook.run(sql)
        logging.info("SL_FACT_PASSENGER 생성/적재 완료")

    dim_time = create_dim_time()
    dim_gate = create_dim_gate()
    fact = create_fact_passenger()

    [dim_time, dim_gate] >> fact
