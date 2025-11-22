from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from SlackAlert import send_slack_success_callback, send_slack_failure_callback

default_args = {
    'owner': 'dahye',
    'depends_on_past': False,
    'retries': 1,
    "retry_delay": timedelta(seconds=60)
}

with DAG(
    'master_airport_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    on_success_callback=send_slack_success_callback,
    on_failure_callback=send_slack_failure_callback
) as dag:

    # 1. airport_all_departure.csv preprocessing
    dag1 = TriggerDagRunOperator(
        task_id='trigger_airport_schedule_preprocessing',
        trigger_dag_id='airport_schedule_preprocessing',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # 2. preprocessed airport_all_departure.csv load to S3
    dag2 = TriggerDagRunOperator(
        task_id='trigger_upload_csv_to_s3',
        trigger_dag_id='upload_csv_to_s3',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # 3. S3 to Snowflake
    dag3 = TriggerDagRunOperator(
        task_id='trigger_s3_to_snowflake',
        trigger_dag_id='s3_to_snowflake',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # 4. Snowflake : Bronze layer > Silver layer
    dag4 = TriggerDagRunOperator(
        task_id='trigger_silver_layer_generation',
        trigger_dag_id='silver_layer_generation',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # 5. Snowflake : Silver layer > Gold layer
    dag5 = TriggerDagRunOperator(
        task_id='trigger_gold_layer_generation',
        trigger_dag_id='gold_layer_generation',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # dags process
    dag1 >> dag2 >> dag3 >> dag4 >> dag5
