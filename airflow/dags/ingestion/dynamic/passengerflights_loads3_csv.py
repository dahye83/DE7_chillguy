from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.decorators import task
from airflow import DAG

import logging
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from io import StringIO

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

@task(multiple_outputs = True)
def extract():
    items1 = get_flight_data('arrival')
    items2 = get_flight_data('departure')

    logging.info('Passenger Flights Data Extract Complete')
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
    columns = ['AIRLINE', 'FLIGHTID', 'SCHEDULEDATETIME', 'ESTIMATEDDATETIME',
               'AIRPORT', 'GATENUMBER', 'CAROUSEL', 'EXITNUMBER', 'CODESHARE',
               'MASTERFLIGHTID', 'REMARK', 'AIRPORTCODE', 'TERMINALID',
               'TYPEOFFLIGHT', 'FID', 'FSTANDPOSITION']

    itemsToList = transform(items)
    print(items[:10])
    df = pd.DataFrame(itemsToList, columns = columns)
    # 필요없는 열 제거
    # '', None -> np.nan 변경
    # master 비행기의 masterflightid를 자신의 id로 변경
    # remark == NaN 제거
    # 결항, 회항이 아닌데 NaN이 있는 데이터 제거 -> 결항, 회항 NaN 유지
    df = df.drop(['CAROUSEL', 'FID', 'FSTANDPOSITION'], axis = 1)
    df = df.replace(['', None], np.nan)
    cond = df['CODESHARE'] == 'Master'
    df.loc[cond, 'MASTERFLIGHTID'] = df.loc[cond, 'FLIGHTID']
    df = df[df['REMARK'].notna()]
    df = df[~((df['REMARK'] != '결항') & (df.isna().any(axis = 1)))]

    logging.info('Transform Arrival Data Complete')
    return df

@task
def transform_departure(items):
    columns = ['AIRLINE', 'FLIGHTID', 'SCHEDULEDATETIME', 'ESTIMATEDDATETIME',
               'AIRPORT', 'CHKINRANGE', 'GATENUMBER', 'CODESHARE',
               'MASTERFLIGHTID', 'REMARK', 'AIRPORTCODE', 'TERMINALID',
               'TYPEOFFLIGHT', 'FID', 'FSTANDPOSITION']
    
    itemsToList = transform(items)
    print(items[:10])
    df = pd.DataFrame(itemsToList, columns = columns)
    # 필요없는 열 제거
    # '', None -> np.nan 변경
    # master 비행기의 masterflightid를 자신의 id로 변경
    # 결항이 아닌데 NaN이 있는 데이터 제거 -> 결항 NaN 유지
    df = df.drop(['FID', 'FSTANDPOSITION'], axis = 1)
    df = df.replace(['', None], np.nan)
    cond = df['CODESHARE'] == 'Master'
    df.loc[cond, 'MASTERFLIGHTID'] = df.loc[cond, 'FLIGHTID']
    df = df[~((df['REMARK'] != '결항') & (df.isna().any(axis = 1)))]

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
        Key = 'dynamic/passenger_arrivals_daily.csv'
    )

    conn.put_object(
        Body = csvBuffer2.getvalue(),
        Bucket = Variable.get('bucket_name'),
        Key = 'dynamic/passenger_departures_daily.csv'
    )

    logging.info('Passenger Flights Data Load Complete')

with DAG(
    dag_id = 'passenger_flights_to_s3',
    start_date = datetime(2025, 11, 17),
    schedule = '0 5 * * *', # 14(5 + 9)시 스케줄링
    catchup = False,
    default_args = {
        'retries': 3,
        'retry_delay' : timedelta(minutes = 3)
    }
) as dag:
    
    items = extract()
    items1, items2 = items['items1'], items['items2']
    df1 = transform_arrival(items1)
    df2 = transform_departure(items2)
    load(df1, df2)
