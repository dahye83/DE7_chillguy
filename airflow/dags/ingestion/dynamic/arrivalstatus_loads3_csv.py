from airflow.decorators import task
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

import logging
import requests
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime, timedelta

def get_s3_conn():
    hook = S3Hook(aws_conn_id = 's3_conn_id')
    conn = hook.get_conn()

    logging.info('Get S3 Connection')
    return conn

def get_arrivals_data(terno):
    # 데이터 출처
    # https://www.data.go.kr/data/15095061/openapi.do
    URL = 'http://apis.data.go.kr/B551177/StatusOfArrivals/getArrivalsCongestion'
    serviceKey = Variable.get('byeong_serviceKey')
    numOfRows = 100
    pageNo = 1
    ternoT1 = 'T1'
    ternoT2 = 'T2'
    params = {
        'serviceKey': serviceKey,
        'numOfRows': numOfRows,
        'pageNo': pageNo,
        'terno': ternoT1,
        'type': 'json'
    }

    if terno == 'T2':
        params['terno'] = ternoT2

    response = requests.get(URL, params = params)
    items = response.json()['response']['body']['items']

    return items

@task
def extract():
    # 1, 2번 터미널 개별 데이터 수집
    i1 = get_arrivals_data('T1')
    i2 = get_arrivals_data('T2')
    items = i1 + i2

    logging.info('Extract Complete')
    
    return items

@task
def transform(items):
    columns = ['TERNO', 'ENTRYGATE', 'KOREAN', 'FOREIGNER',
               'SCHEDULETIME', 'ESTIMATEDTIME', 'AIRPORT',
               'GATENUMBER', 'FLIGHTID']
    
    # dict -> list 형태 변경
    itemsToList = []
    for i in items:
        temp = list(i.values())
        itemsToList.append(temp)

    df = pd.DataFrame(itemsToList, columns = columns)
    # 빈 값 결측치 처리 후 제거
    df = df.replace('', np.nan)
    df = df.dropna()

    logging.info('Transform Complete')

    return df

@task
def load(df):
    conn = get_s3_conn()

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index = False)
    csv_buffer.seek(0)

    conn.put_object(
        Body = csv_buffer.getvalue(),
        Bucket = Variable.get('bucket_name'),
        Key = f'dynamic/arrival_status_daily.csv'
    )

    logging.info('S3 Load Complete')

with DAG(
    dag_id = 'arrival_status_to_s3',
    start_date = datetime(2025, 11, 17),
    schedule = '0 5 * * *', # 14(5 + 9)시 스케줄링
    catchup = False,
    default_args = {
        'retries': 3,
        'retry_delay': timedelta(minutes = 3)
    }
) as dag:
    
    data = extract()
    df = transform(data)
    load(df)
