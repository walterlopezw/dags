from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
import encodings.idna
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


#export AIRFLOW_CONN_SLACK_DEFAULT='slack://:token@'
def on_failure_callback():
	SLACK_CONN_ID = 'slack_conn'
	slack_webhook_token=BaseHook.get_connection(SLACK_CONN_ID).password
	slack_notification =SlackWebhookOperator(
		task_id="slack_notification",
		http_conn_id=SLACK_CONN_ID,
		webhook_token=slack_webhook_token,
		username='alerts',
        message="Error")
	return slack_notification.execute("error2")

default_args = {
    'owner': 'WALII',
    'depends_on_past': False,
    'email': ['walterleonardo@gmail.com'],
    'on_failure_callback': on_failure_callback,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    'DAG_airflow_slack_notification_tutorial',
    default_args=default_args,
    description='A simple slack alert ',
    schedule_interval='@daily',
    start_date=datetime(2021,1,1),
    catchup=False
) as dag:
    semd_notification = PythonOperator(task_id="slack_notification", python_callable=on_failure_callback)
    # bash_task = BashOperator(
    #     task_id='simple_bash_task',
    #     bash_command='echo "TESTS"')
    
    semd_notification
