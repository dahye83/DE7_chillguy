import json
import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
import requests

# 변수
BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_CONN_ID = "s3_conn_id"

BASE_URL = "https://www.airport.kr/dep/ap_ko/getDepPasSchList.do"

default_args = {
    "owner": "sihyun",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

@dag(
    dag_id="airportinfo_departure_7days_to_s3",
    description="인천공항 출발 정보 7일 수집 후 S3(dynamic/) 저장",
    schedule="0 5 * * *",
    start_date=datetime(2025, 11, 17),
    catchup=False,
    default_args=default_args,
    tags=["airport", "7days", "s3"],
)
def dag_airportinfo_departure_7days():

    # 1) 단일 날짜 조회
    def fetch_one_day(date_str):

        headers = {
            "accept": "*/*",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://www.airport.kr",
            "referer": "https://www.airport.kr/ap_ko/869/subview.do",
            "user-agent": "Mozilla/5.0",
            "x-requested-with": "XMLHttpRequest",
        }

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
            "arrOrDep": "D",
            "page": "1",
            "row": "2000",
        }

        res = requests.post(BASE_URL, headers=headers, data=form_payload)
        res.raise_for_status()

        data = res.json()
        return data.get("scheduleList", [])

    # 2) 7일치 조회
    @task
    def fetch_7days():
        all_rows = []

        for i in range(7):
            d = (datetime.today() + timedelta(days=i)).strftime("%Y%m%d")
            rows = fetch_one_day(d)

            for r in rows:
                r["date"] = d  # ⭐ 날짜 추가

            all_rows.extend(rows)

        return json.dumps(all_rows, ensure_ascii=False)

    # 3) 한국어 컬럼 + 날짜 + 목적지코드 포함 → S3 저장
    @task
    def save_to_s3(json_text: str):
        rows = json.loads(json_text)

        if not rows:
            raise ValueError("❌ 저장할 데이터가 없습니다.")

        df = pd.DataFrame(rows)

        # 한국어 컬럼명 + 목적지 코드 추가
        df_out = pd.DataFrame({
            "날짜": df.get("date"),
            "출발시간": df.get("stime"),
            "목적지": df.get("airportName1Ko"),
            "목적지코드(IATA)": df.get("p1code"),   
            "운항편명/항공사": df.get("fnumber") + " / " + df.get("airlineNameKo"),
            "터미널": df.get("terminal"),
            "체크인 카운터": df.get("chkinrange"),
            "탑승구": df.get("gatenumber"),
            "출발현황": df.get("stattxt"),
        })

        csv_str = df_out.to_csv(index=False, encoding="utf-8-sig")

        filename = f"dynamic/airportinfo_{datetime.today().strftime('%Y%m%d')}_7days.csv"

        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        s3.load_string(
            string_data=csv_str,
            key=filename,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        print(f"📤 저장완료 → s3://{BUCKET_NAME}/{filename}")
        return filename

    save_to_s3(fetch_7days())


dag_airportinfo_departure_7days()
