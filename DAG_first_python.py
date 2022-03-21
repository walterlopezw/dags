from datetime import timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import logging
from airflow.hooks.base_hook import BaseHook
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
SLACK_CONN_ID = 'slack'

SlackAPIPostOperator(
    task_id='failure',
    token='xapp-1-A035WUU1B1V-3209040803046-133211c741abae906d0e233ecd0806e070fc94c5cc800f51732a5696d9016c05',
    text='Hello World !',
    channel='Alerts',  # Replace with your Slack username
    username='airflow'
)



def scrape():
    logging.info("Screapeando...")

def process():
    logging.info("Processing...")

def save():
    logging.info("Saving...")

def slack_failed_task(context):  
    failed_alert = SlackAPIPostOperator(
        task_id='slack_failed',
        channel="#alerts",
        token="xapp-1-A035WUU1B1V-3209040803046-133211c741abae906d0e233ecd0806e070fc94c5cc800f51732a5696d9016c05",
        text = ':red_circle: Task Failed',
        username = 'airflow',)
    return failed_alert.execute(context=context)

def task_fail_slack_alert(context):
    """
    Sends message to a slack channel.
    If you want to send it to a "user" -> use "@user",
        if "public channel" -> use "#channel",
        if "private channel" -> use "channel"
    """
    slack_channel = BaseHook.get_connection(SLACK_CONN_ID).login
    slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    failed_alert = SlackAPIPostOperator(
        task_id='slack_failed',
        channel=slack_channel,
        token=slack_token,
        text="""
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    )
    return failed_alert.execute(context=context)


def on_failure_callback(context):
	SLACK_CONN_ID = 'slack_conn'
	slack_webhook_token=BaseHook.get_connection(SLACK_CONN_ID).password
	slack_notification =SlackWebhookOperator(
		task_id="slack_notification",
		http_conn_id=SLACK_CONN_ID,
		webhook_token=slack_webhook_token,
		username='airflow')
	return slack_notification.execute(context=context)





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
    'DAG_tutorial_python_alerts',
    default_args=default_args,
    description='A simple tutorial DAG python',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    scrape_task = PythonOperator(task_id="scrape", python_callable=scrape)
    process_task = PythonOperator(task_id="process", python_callable=process)
    save_task = PythonOperator(task_id="save", python_callable=save)
    # task_with_failed_slack_alerts = BashOperator(
    #     task_id='fail_task',
    #     bash_command='exit 1',
    #     on_failure_callback=task_fail_slack_alert)
    bash_task =BashOperator(
        task_id='simple_bash_task',
        bash_command='echo "TESTS"')

    bash_task >> scrape_task >> process_task >> save_task