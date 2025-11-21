from airflow.models import Variable

from datetime import datetime, timedelta
import requests
import logging

URL = Variable.get('slack_alert_url')
HEADERS = {
    'Content-type': 'applycation/json'
}

def send_message(json):
    response = requests.post(URL, headers = HEADERS, json = json)
    if response.status_code == 200:
        logging.info('슬랙 전송 성공')
    else:
        logging.info('슬랙 전송 실패')    

def send_slack_failure_callback(context):
    execution_date = (context.get('execution_date') + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    exception = context.get('exception')

    data = f'''
:rotating_light: DAG 실패
일시 : {execution_date}
• DAG: {dag_id}
• Task: {task_id}
```{exception}```
'''    
    json = {'text': data}
    send_message(json)

def send_slack_success_callback(context):
    execution_date = (context.get('execution_date') + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')
    dag_id = context.get('dag').dag_id

    data = f'''
:white_check_mark: DAG 성공
일시 : {execution_date}
• DAG: {dag_id}
'''
    
    json = {'text': data}
    send_message(json)
