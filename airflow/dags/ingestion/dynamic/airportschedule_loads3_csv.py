import json
import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests
import traceback   

#slack 설정 
SLACK_WEBHOOK_URL = Variable.get("slack_webhook_url")

def slack_on_failure(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    exception = context.get("exception")

    error_msg = "".join(
        traceback.format_exception(None, exception, exception.__traceback__)
    ) if exception else "No exception info"

    message = (
        f":red_circle: DAG *{dag_id}* Task *{task_id}* 실패\n"
        f"Execution Date: {execution_date}\n"
        f"Error:\n```{error_msg}```\n"
        f"Log: {log_url}"
    )
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})


def slack_on_success(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")

    message = (
        f":white_check_mark: DAG *{dag_id}* Task *{task_id}* 실행 성공 ✅\n"
        f"Execution Date: {execution_date}"
    )
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})

# S3 설정
BUCKET_NAME = Variable.get("BUCKET_NAME")
S3_CONN_ID = "s3_conn_id"
BASE_URL = Variable.get("BASE_URL")

default_args = {
    "owner": "sihyun",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

@dag(
    dag_id="airport_schedule_to_s3",
    description="인천공항 출발 정보 7일 수집 후 S3(dynamic/) 저장",
    schedule="0 5 * * *",  
    start_date=datetime(2025, 11, 17),
    catchup=False,
    default_args=default_args,
    tags=["airport", "7days", "s3"],
)
def dag_airportinfo_departure_7days():

    # 단일 날짜 조회
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

    @task(
        on_failure_callback=slack_on_failure,
        on_success_callback=slack_on_success
    )
    def fetch_7days():
        all_rows = []
        for i in range(7):
            d = (datetime.today() + timedelta(days=i)).strftime("%Y%m%d")
            rows = fetch_one_day(d)
            for r in rows:
                r["date"] = d
            all_rows.extend(rows)
        return all_rows

    @task(
        on_failure_callback=slack_on_failure,
        on_success_callback=slack_on_success
    )
    def upload_to_s3(rows):
        if not rows:
            raise ValueError("❌ 저장할 데이터가 없습니다.")

        df = pd.DataFrame(rows)

        df_out = pd.DataFrame({
            "DATE": df.get("date"),
            "TIME": df.get("stime"),
            "AIRPORT": df.get("airportName1Ko"),
            "IATA_CODE": df.get("p1code"),
            "FLIGHT" : df.get("fnumber"),
            "AIRLINE": df.get("airlineNameKo"),
            "TERMINAL": df.get("terminal"),
            "CHECKIN COUNTER": df.get("chkinrange"),
            "GATE": df.get("gatenumber"),
            "DSTATUS": df.get("stattxt"),
        })

        csv_str = df_out.to_csv(index=False, encoding="utf-8-sig")
        filename = "airportschedule_loads3_csv"

        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        s3.load_string(
            string_data=csv_str,
            key=filename,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        return filename

    trigger_snowflake = TriggerDagRunOperator(
        task_id="trigger_snowflake_dag",
        trigger_dag_id="airport_schedule_to_snowflake",
        on_failure_callback=slack_on_failure,
        on_success_callback=slack_on_success   
    )


    upload_to_s3(fetch_7days()) >> trigger_snowflake

dag_airportinfo_departure_7days()
