from datetime import timedelta,datetime
from textwrap import dedent
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from airflow.operators.bash import BashOperator   
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'itms_segments_traffic',
    default_args=default_args,
    description='Surat ITMS Traffic Segments',
    schedule_interval="*/15 2-17 * * *",
    start_date=datetime(2022, 2, 28),
    tags=['surat','itms','sp2','itms_segments_traffic'],
) as dag:
    submit_job = SparkSubmitOperator(application="/opt/airflow/dags/itms_segments_traffic.py", task_id="itms_segments_traffic", packages="org.apache.kudu:kudu-spark3_2.12:1.15.0", conn_id="spark_service", executor_cores=1, total_executor_cores=2)
