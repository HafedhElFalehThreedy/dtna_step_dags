from airflow.models.dag import DAG
from airflow.operators.bash_operator import BashOperator
import os
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'convert_to_step',
    default_args=default_args,
    description='A DAG to convert all references to mono-step file',
    # schedule_interval=timedelta(days=1),
)

 

# Define the PythonOperator
convert_to_step_task = BashOperator(
    task_id='convert_to_step_task',
    bash_command='echo Hello world!! -- convert_to_step_task',
    dag=dag,
)

# Set task dependencies
convert_to_step_task