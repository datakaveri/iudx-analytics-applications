from datetime import timedelta,datetime
from textwrap import dedent
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator   
from airflow.utils.dates import days_ago

# [END import_module] surat_itms_job_2.py
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 0
}
with DAG(
    'Surat_ITMS_Trips',
    default_args=default_args,
    description='Surat ITMS data',
    schedule_interval="*/15 1-17 * * *",
    start_date=datetime(2022, 2, 24),
    tags=['surat','itms','sp1'],
) as dag:
    submit_job = SparkSubmitOperator(application="/opt/airflow/dags/surat_itms_job.py", task_id="submit_job", packages="org.apache.kudu:kudu-spark3_2.12:1.15.0", conn_id="spark_service", executor_cores=1, total_executor_cores=2)
