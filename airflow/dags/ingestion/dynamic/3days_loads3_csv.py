from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.models import Variable
from airflow.decorators import task
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from SlackAlert import send_slack_success_callback, send_slack_failure_callback

import logging
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from io import StringIO, BytesIO

def get_s3_conn():
    hook = S3Hook(aws_conn_id = 's3_conn_id')
    conn = hook.get_conn()

    logging.info('Get S3 Connection')
    return conn

def get_flight_data(key):
    # 데이터 출처
    # https://www.data.go.kr/data/15112968/openapi.do
    URL = 'http://apis.data.go.kr/B551177/StatusOfPassengerFlightsDeOdp/'

    if key == 'arrival':
        URL = URL + 'getPassengerArrivalsDeOdp'
    else:
        URL = URL + 'getPassengerDeparturesDeOdp'
    
    serviceKey = Variable.get('byeong_serviceKey')
    pageNo = 1
    numOfRows = 15000
    params = {
        'serviceKey': serviceKey,
        'pageNo': pageNo,
        'numOfRows': numOfRows,
        'type': 'json'
    }

    response = requests.get(url = URL, params = params)
    items = response.json()['response']['body']['items']

    logging.info('Get Data')
    return items

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
            def map_dtype_to_snowflake(dtype):
                if pd.api.types.is_integer_dtype(dtype):
                    return "NUMBER"
                elif pd.api.types.is_float_dtype(dtype):
                    return "FLOAT"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    return "TIMESTAMP_NTZ"
                else:
                    return "STRING"
    
            # 컬럼 생성
            create_cols = ", ".join([f'{c} {map_dtype_to_snowflake(dtype)}' for c, dtype in df.dtypes.items()])
            # 3️⃣ 테이블 생성
            print(create_cols)
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

@task(multiple_outputs = True)
def extract():
    items1 = get_flight_data('arrival')
    items2 = get_flight_data('departure')

    logging.info('3days Flights Data Extract Complete')
    return {'items1': items1, 'items2': items2}

def transform(items):
    itemsToList = []
    for i in items:
        # 미래 도착 예정 / 미래 출발 예정 데이터는 입력하지 않음
        if i['estimatedDateTime'] > datetime.now().strftime('%Y%m%d%H%M'):
            break

        temp = list(i.values())
        itemsToList.append(temp)

    logging.info('Transform')
    return itemsToList

@task
def transform_arrival(items):
    columns = ['AIRLINE', 'FLIGHT_CD', 'SCHEDULED_TM', 'ESTIMATED_TM',
               'CITY_KOR', 'GATE', 'CAROUSEL', 'EXIT', 'CODESHARE',
               'MASTERID', 'STATUS', 'IATA_CD', 'TERMINAL',
               'FLIGHT_TYPE', 'FID', 'FSTANDPOSITION']

    itemsToList = transform(items)
    print(items[:10])
    df = pd.DataFrame(itemsToList, columns = columns)
    # 필요없는 열 제거
    # '', None -> np.nan 변경
    # master 비행기의 masterflightid를 자신의 id로 변경
    # remark == NaN 제거
    # 결항, 회항이 아닌데 NaN이 있는 데이터 제거 -> 결항, 회항 NaN 유지
    df = df.drop(['CITY_KOR', 'CAROUSEL', 'FID', 'FSTANDPOSITION'], axis = 1)
    df = df.replace(['', None], np.nan)
    cond = df['CODESHARE'] == 'Master'
    df.loc[cond, 'MASTERID'] = df.loc[cond, 'FLIGHT_CD']
    df = df[df['STATUS'].notna()]
    df = df[~((df['STATUS'] != '결항') & (df.isna().any(axis = 1)))]
    # 컬럼 순서 변경
    df = df[['AIRLINE', 'IATA_CD', 'FLIGHT_CD', 'TERMINAL',
             'GATE', 'EXIT', 'CODESHARE', 'MASTERID',
             'SCHEDULED_TM', 'ESTIMATED_TM', 'STATUS', 'FLIGHT_TYPE']]
    # 시간 형식 변경
    df['SCHEDULED_TM'] = pd.to_datetime(df['SCHEDULED_TM'], format = '%Y%m%d%H%M')
    df['ESTIMATED_TM'] = pd.to_datetime(df['ESTIMATED_TM'], format = '%Y%m%d%H%M')    

    logging.info('Transform Arrival Data Complete')
    return df

@task
def transform_departure(items):
    columns = ['AIRLINE', 'FLIGHT_CD', 'SCHEDULED_TM', 'ESTIMATED_TM',
               'CITY_KOR', 'CHECKIN_COUNTER', 'GATE', 'CODESHARE',
               'MASTERID', 'STATUS', 'IATA_CD', 'TERMINAL',
               'FLIGHT_TYPE', 'FID', 'FSTANDPOSITION']
    
    itemsToList = transform(items)
    print(items[:10])
    df = pd.DataFrame(itemsToList, columns = columns)
    # 필요없는 열 제거
    # '', None -> np.nan 변경
    # master 비행기의 masterflightid를 자신의 id로 변경
    # 결항이 아닌데 NaN이 있는 데이터 제거 -> 결항 NaN 유지
    df = df.drop(['CITY_KOR', 'FID', 'FSTANDPOSITION'], axis = 1)
    df = df.replace(['', None], np.nan)
    cond = df['CODESHARE'] == 'Master'
    df.loc[cond, 'MASTERID'] = df.loc[cond, 'FLIGHT_CD']
    df = df[~((df['STATUS'] != '결항') & (df.isna().any(axis = 1)))]
    # 컬럼 순서 변경
    df = df[['AIRLINE', 'IATA_CD', 'FLIGHT_CD', 'TERMINAL',
             'GATE', 'CHECKIN_COUNTER', 'CODESHARE', 'MASTERID',
             'SCHEDULED_TM', 'ESTIMATED_TM', 'STATUS', 'FLIGHT_TYPE']]
    # 시간 형식 변경
    df['SCHEDULED_TM'] = pd.to_datetime(df['SCHEDULED_TM'], format = '%Y%m%d%H%M')
    df['ESTIMATED_TM'] = pd.to_datetime(df['ESTIMATED_TM'], format = '%Y%m%d%H%M')

    logging.info('Transform Departure Data Complete')
    return df

@task
def load(df1, df2):
    conn = get_s3_conn()
    
    csvBuffer1 = StringIO()
    csvBuffer2 = StringIO()
    df1.to_csv(csvBuffer1, index = False)
    df2.to_csv(csvBuffer2, index = False)
    csvBuffer1.seek(0)
    csvBuffer2.seek(0)

    conn.put_object(
        Body = csvBuffer1.getvalue(),
        Bucket = Variable.get('bucket_name'),
        Key = 'dynamic/3days_arrivals_daily.csv'
    )

    conn.put_object(
        Body = csvBuffer2.getvalue(),
        Bucket = Variable.get('bucket_name'),
        Key = 'dynamic/3days_departures_daily.csv'
    )

    logging.info('3days Flights Data Load Complete')

with DAG(
    dag_id = '3days_to_s3',
    start_date = datetime(2025, 11, 17),
    schedule = '0 5 * * *', # 14(5 + 9)시 스케줄링
    catchup = False,
    default_args = {
        'retries': 3,
        'retry_delay' : timedelta(minutes = 3),
        'on_failure_callback': send_slack_failure_callback
    },
    on_success_callback = send_slack_success_callback,
) as dag:
    
    items = extract()
    items1, items2 = items['items1'], items['items2']
    df1 = transform_arrival(items1)
    df2 = transform_departure(items2)
    load_task = load(df1, df2)

    DATABASE = 'TEAM_7_CHILL'
    SCHEMA = 'BRONZE'

    snowflake_task1 = create_s3_to_snowflake_task(
        task_id = '3days_arrival_to_snowflake',
        s3_bucket = Variable.get('bucket_name'),
        s3_key = 'dynamic/3days_arrivals_daily.csv',
        snowflake_conn_id = 'snowflake_conn_id',
        database = DATABASE,
        schema = SCHEMA,
        table_name = 'BR_3DAYS_ARRIVALS_DAILY'
    )

    snowflake_task2 = create_s3_to_snowflake_task(
        task_id = '3days_departure_to_snowflake',
        s3_bucket = Variable.get('bucket_name'),
        s3_key = 'dynamic/3days_departures_daily.csv',
        snowflake_conn_id = 'snowflake_conn_id',
        database = DATABASE,
        schema = SCHEMA,
        table_name = 'BR_3DAYS_DEPARTURE_DAILY'
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id = 'next_dag_trigger',
        trigger_dag_id = '3dyas_create_sl_gd_table'
    )

    load_task >> snowflake_task1 >> snowflake_task2 >> trigger_next_dag

