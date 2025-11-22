import os
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from SlackAlert import send_slack_success_callback, send_slack_failure_callback

# Path Settings
DATA_DIR = "/opt/airflow/data"
RAW_CSV_PATH = os.path.join(DATA_DIR, "airport_all_departure.csv")

PREPROCESS_DIR = os.path.join(DATA_DIR, "preprocessed")
os.makedirs(PREPROCESS_DIR, exist_ok=True)

STEP1_PATH = os.path.join(PREPROCESS_DIR, "step1_add_columns.csv")
STEP2_PATH = os.path.join(PREPROCESS_DIR, "step2_filled.csv")
STEP3_PATH = os.path.join(PREPROCESS_DIR, "step3_selected.csv")
FINAL_CSV_PATH = os.path.join(PREPROCESS_DIR, "airport_preprocessed.csv")


# Utility Functions
def load_csv(path):
    """CSV 파일을 UTF-8 인코딩으로 로딩"""
    return pd.read_csv(path, encoding="utf-8-sig")


def save_csv(df, path):
    """DataFrame을 CSV로 저장"""
    df.to_csv(path, index=False, encoding="utf-8-sig")


# ============================================================
# Step 1 — Add Columns
# ============================================================

# 1) column : delay time
def preprocess_delay_time(df):
    """
    지연시간 컬럼 추가
    - 1. 계획시간/예상시간/출발시간을 분(minute)으로 변환
    - 2. 지연상태에 따라 지연시간 컬럼을 계산하여 추가
    """
    def to_minutes(t):
        if pd.isna(t) or t == ":":
            return None
        h, m = t.split(":")
        return int(h) * 60 + int(m)

    df["계획분"] = df["계획시간"].apply(to_minutes)
    df["예상분"] = df["예상시간"].apply(to_minutes)
    df["실제분"] = df["출발시간"].apply(to_minutes)

    def calc_delay(row):
        if row["상태"] != "지연":
            return 0

        plan, expected, actual = row["계획분"], row["예상분"], row["실제분"]

        if plan is None:
            return 0

        if expected is not None and expected == plan:
            if actual is None:
                return 0
            return max(actual - plan, 0)

        if expected is not None:
            return max(expected - plan, 0)

        if actual is not None:
            return max(actual - plan, 0)

        return 0

    df["지연시간"] = df.apply(calc_delay, axis=1)
    return df

# 2) columns : weekday, time slot
def add_date_features(df):
    """
    - 1. 일자컬럼을 이용하여 요일 컬럼추가
    - 2. 계획시간(HH:MM)에서 '시(HH)'만 추출하여 시간대 컬럼 생성
    """
    df["일자_dt"] = pd.to_datetime(df["일자"], format="%Y%m%d")
    df["요일"] = df["일자_dt"].dt.day_name()

    weekday_map = {
        "Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일",
        "Thursday": "목요일", "Friday": "금요일", "Saturday": "토요일",
        "Sunday": "일요일",
    }
    df["요일"] = df["요일"].map(weekday_map)

    def extract_hour(t):
        if pd.isna(t) or t == ":":
            return None
        return int(t.split(":")[0])

    df["시간대"] = df["계획시간"].apply(extract_hour)
    return df

# 3) column : codeshare
def label_codeshare(df):
    """
    - 1. 동일한 조건의 컬럼을 가진 경우에서 편명이 다르면 코드쉐어로 판단
    - 2. is_codeshare, codeshare_group_id 생성
    """
    key_cols = ["도착지", "계획시간", "일자", "구분", "상태"]
    df["is_codeshare"] = False
    df["codeshare_group_id"] = pd.NA

    group_id = 0
    for _, group in df.groupby(key_cols):
        if group["항공사"].nunique() > 1 or group["편명"].nunique() > 1:
            df.loc[group.index, "is_codeshare"] = True
            df.loc[group.index, "codeshare_group_id"] = group_id
            group_id += 1

    return df

# Step 1 — 컬럼 추가 : 지연시간, 요일, 시간대, 코드쉐어
def add_columns(**context):
    df = load_csv(RAW_CSV_PATH)
    df = df[df["구분"] == "여객"].copy()

    df = preprocess_delay_time(df)
    df = add_date_features(df)
    df = label_codeshare(df)

    save_csv(df, STEP1_PATH)


# ============================================================
# Step 2 — Missing Value Imputation
# ============================================================
def missing_value_imputation(**context):
    """
    결측값을 고정값으로 매핑하여 처리 
    """
    df = load_csv(STEP1_PATH)

    airline_from_flight = {
        "KL855": "네덜란드항공",
        "HA1872": "하와이안항공",
        "AXY218P": "에어엑스차터",
        "AXY496H": "에어엑스차터",
    }
    for flight, air in airline_from_flight.items():
        df.loc[df["항공사"].isna() & (df["편명"] == flight), "항공사"] = air

    airline_dest = {
        "산동항공": "우이산",
        "센트럼항공": "타슈켄트",
        "스카이 앙코르 항공": "크라체",
        "에어아스타나항공": "알마티",
        "에어프레미아": "주바",
        "우즈베키스탄항공": "타슈켄트",
        "이스타항공": "알마티",
        "진에어": "엔시",
        "카놋샤크항공": "타슈켄트",
        "티웨이항공": "타슈켄트",
        "대한항공": "크라체",
    }

    mask_null_dest = df["도착지"].isna()
    for airline, dest in airline_dest.items():
        df.loc[mask_null_dest & (df["항공사"] == airline), "도착지"] = dest

    asiana_dest = {
        "OZ739": "크라체", "OZ740": "크라체",
        "OZ573": "타슈켄트", "OZ574": "타슈켄트",
        "OZ577": "알마티",   "OZ5775": "알마티", "OZ578": "알마티",
    }
    for flight, dest in asiana_dest.items():
        df.loc[
            mask_null_dest
            & (df["항공사"] == "아시아나항공")
            & (df["편명"] == flight),
            "도착지",
        ] = dest

    save_csv(df, STEP2_PATH)


# ============================================================
# Step 3 — Select & Rename Columns
# ============================================================
def select_and_rename_columns(**context):
    """
    - 1. 필요한 컬럼 선택
    - 2. boolean type 변환
    - 3. 컬럼명 변경
    - 4. 최종 csv 파일 저장
    """
    df = load_csv(STEP2_PATH)

    # 1) 필요한 컬럼 선택
    selected_cols = [
        "출발/도착", "공항명", "항공사", "편명", "도착지", "일자",
        "요일", "시간대", "계획시간", "예상시간", "출발시간",
        "구분", "상태", "지연원인", "지연시간", "is_codeshare",
    ]
    df = df[selected_cols]

    # 2) Boolean type 변환
    df["CODESHARE_YN"] = df["is_codeshare"].map(lambda x: "Y" if x else "N")
    df.drop(columns=["is_codeshare"], inplace=True)

    # 3) 컬럼명 영문으로 변경
    rename_map = {
        '출발/도착': 'DIRECTION',
        '공항명': 'AIRPORT_NAME',
        '항공사': 'AIRLINE',
        '편명': 'FLIGHT_NUMBER',
        '도착지': 'DESTINATION',
        '일자': 'FLIGHT_DATE',
        '요일': 'WEEKDAY',
        '시간대': 'TIME_SLOT',
        '계획시간': 'SCHEDULED_TIME',
        '예상시간': 'ESTIMATED_TIME',
        '출발시간': 'DEPARTURE_TIME',
        '구분': 'CATEGORY',
        '상태': 'STATUS',
        '지연원인': 'DELAY_REASON',
        '지연시간': 'DELAY_TIME',
        'CODESHARE_YN': 'CODESHARE_YN',
    }

    df.rename(columns=rename_map, inplace=True)

    # 4) 최종 csv file 저장
    save_csv(df, FINAL_CSV_PATH)



# ============================================================
# DAG Definition
# ============================================================
default_args = {
    'owner': 'dahye',
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
}

with DAG(
    dag_id="airport_schedule_preprocessing",
    start_date=days_ago(1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="add_columns",
        python_callable=add_columns,
        on_success_callback=send_slack_success_callback,
        on_failure_callback=send_slack_failure_callback,
    )

    task2 = PythonOperator(
        task_id="missing_value_imputation",
        python_callable=missing_value_imputation,
        on_success_callback=send_slack_success_callback,
        on_failure_callback=send_slack_failure_callback,
    )

    task3 = PythonOperator(
    task_id="select_and_rename_columns",
    python_callable=select_and_rename_columns,
    on_success_callback=send_slack_success_callback,
    on_failure_callback=send_slack_failure_callback
)

task1 >> task2 >> task3
