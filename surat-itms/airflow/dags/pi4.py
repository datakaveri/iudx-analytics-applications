from datetime import timedelta,datetime
from textwrap import dedent
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator   
from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'Train_Traffic_Delay',
    default_args=default_args,
    description='Surat ITMS Bus Traffic Delay Train Model',
    schedule_interval="0 8 * * SUN",
    start_date=datetime(2022, 2, 24),
    tags=['surat','itms','sp2','segmentation', 'train_traffic_delay'],
) as dag:
    submit_job = SparkSubmitOperator(application="/opt/airflow/dags/train_traffic_delay.py", task_id="train_traffic_delay", packages="org.apache.kudu:kudu-spark3_2.12:1.15.0", conn_id="spark_service", executor_cores=1, total_executor_cores=2)
