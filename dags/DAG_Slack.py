
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.slack.operators.slack import SlackAPIFileOperator

# [START slack_operator_howto_guide]
with DAG(
    dag_id='slack_example_dag',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args={'slack_conn_id': 'slack', 'channel': '#general', 'initial_comment': 'Hello World!'},
    max_active_runs=1,
    tags=['example'],
) as dag:

    # Send file with filename and filetype
    slack_operator_file = SlackAPIFileOperator(
        task_id="slack_file_upload_1",
        filename="/files/dags/test.txt",
        filetype="txt",
    )

    # Send file content
    slack_operator_file_content = SlackAPIFileOperator(
        task_id="slack_file_upload_2",
        content="file content in txt",
    )
    # [END slack_operator_howto_guide]

    slack_operator_file >> slack_operator_file_content