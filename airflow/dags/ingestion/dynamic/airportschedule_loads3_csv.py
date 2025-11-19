import json
import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests

# 환경 변수
BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_CONN_ID = "s3_conn_id"

# 여객 출발 시간표 API 엔드포인트
BASE_URL = Variable.get("BASE_URL")

default_args = {
    "owner": "sihyun",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

@dag(
    dag_id="airportschedule_loads3_csv",
    description="인천공항 출발 정보 7일 수집 후 S3(dynamic/) 저장",
    schedule="0 5 * * *",  # 2:00 PM 
    start_date=datetime(2025, 11, 17),
    catchup=False,
    default_args=default_args,
    tags=["airport", "7days", "s3"],
)

def dag_airportinfo_departure_7days():

    # 단일 날짜 조회(현재 날짜)
    def fetch_one_day(date_str):

        # 브라우저 실제 요청 헤더 
        headers = {
            "accept": "*/*",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://www.airport.kr",
            "referer": "https://www.airport.kr/ap_ko/869/subview.do",
            "user-agent": "Mozilla/5.0",
            "x-requested-with": "XMLHttpRequest",
        }

        # 요청할 데이터
        form_payload = {
            "siteId": "ap_ko",
            "langSe": "ko",
            "curDate": date_str,
            "daySel": date_str,
            "todayDate": date_str,
            "tomorrowDate": (
                datetime.strptime(date_str, "%Y%m%d") + timedelta(days=1)
            ).strftime("%Y%m%d"),
            "fromTime": "0000",
            "toTime": "2359",
            "arrOrDep": "D", #출발, 도착편 -> "A"
            "page": "1",
            "row": "2000",
        }

        res = requests.post(BASE_URL, headers=headers, data=form_payload)
        res.raise_for_status()

        # 리턴 :원하는 데이터 -> "scheduleList" : []
        data = res.json()
        return data.get("scheduleList", [])

    # 7일치 조회
    @task
    def fetch_7days():
        all_rows = []

        for i in range(7):
            d = (datetime.today() + timedelta(days=i)).strftime("%Y%m%d")
            #fetch_one_day() 7번 반복
            rows = fetch_one_day(d)

            for r in rows:
                r["date"] = d  # 날짜 추가

            all_rows.extend(rows) # 7일 데이터 합치기

        return all_rows

    # 한국어 컬럼 + 날짜 + 목적지코드 포함 → S3
    @task
    def upload_to_s3(rows):

        if not rows:
            raise ValueError("❌ 저장할 데이터가 없습니다.")

        df = pd.DataFrame(rows)

        # 대문자 영문 컬럼명 + 목적지 코드 추가
        df_out = pd.DataFrame({
            "DATE": df.get("date"),
            "TIME": df.get("stime"),
            "AIRPORT": df.get("airportName1Ko"), # 컬럼 통일 위해 목적지 -> 해당 공항 
            "IATA_CODE": df.get("p1code"),        
            "FLIGHT" : df.get("fnumber"),
            "AIRLINE": df.get("airlineNameKo"),
            "TERMINAL": df.get("terminal"),
            "CHECKIN COUNTER": df.get("chkinrange"),
            "GATE": df.get("gatenumber"),
            "DSTATUS": df.get("stattxt"),
        })

        csv_str = df_out.to_csv(index=False, encoding="utf-8-sig")

        filename = "airportschedule_loads3_csv" #파일명 고정

        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        s3.load_string(
            string_data=csv_str,
            key=filename,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        print(f"📤 저장완료 → s3://{BUCKET_NAME}/{filename}")
        return filename

    # ------------------------
    # DAG B(airportschedule_loadsnow_csv) 트리거 추가
    # ------------------------
    trigger_snowflake = TriggerDagRunOperator(
        task_id="trigger_snowflake_dag",
        trigger_dag_id="airportschedule_loadsnow_csv"
    )

    upload_to_s3(fetch_7days()) >> trigger_snowflake  # snowflake dag 실행 연결

dag_airportinfo_departure_7days()