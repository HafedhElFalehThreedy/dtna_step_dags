import random
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import os

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'All-DAGS-Combined_new_pipeline',
    default_args=default_args,
    description='A DAG for processing and converting files',
    # schedule_interval=timedelta(days=1),
)

def set_variable(**kwargs):
    spaceid = kwargs['dag_run'].conf.get('space_id')
    print("The parameter is:", spaceid)
    Variable.set('current_space_id', spaceid)

# PythonOperator to set the variable
set_current_space_id = PythonOperator(
    task_id='set_current_space_id',
    python_callable=set_variable,
    provide_context=True,
    dag=dag,
)

# Define the function to create the global folder
def create_global_folder(current_space_id):
    global_folder_path = f'/opt/airflow/tempSRCfiles/{current_space_id.replace("default/", "")}'  # Construct folder path

    # Check if the folder already exists
    if not os.path.exists(global_folder_path):
        # Create the folder if it doesn't exist
        os.makedirs(global_folder_path)
        print(f"Global folder '{global_folder_path}' created successfully.")
    else:
        print(f"Global folder '{global_folder_path}' already exists.")

create_global_folder_task = PythonOperator(
    task_id='create_global_folder_task',
    python_callable=create_global_folder,
    op_kwargs={'current_space_id': '{{ var.value.current_space_id }}'},
    dag=dag,
)

# Initial trigger task
trigger_export = BashOperator(
    task_id='trigger_export',
    bash_command='echo Initial trigger...',
    dag=dag,
)

# Define the bash command with string manipulation to remove 'default/' prefix
bash_command = """
cd /opt/airflow/tempSRCfiles/export_plmxml/src/ && \
node ./index.js --space_id {{ var.value.current_space_id | replace('default/', '') }} --target_folder /opt/airflow/tempSRCfiles/{{ var.value.current_space_id | replace('default/', '') }} && \
cd /opt/airflow/tempSRCfiles/{{ var.value.current_space_id | replace('default/', '') -l  /opt/airflow/tempSRCfiles/export_plmxml/src/invalid_list.txt }}   
"""

# Subtasks 1: Export to PLMXML
nodejs_space_export_to_plxml = BashOperator(
    task_id='nodejs_space_export_to_plxml',
    bash_command= bash_command,
    dag=dag,
)

def set_latest_plmxml(folder_name):
    # Define the directory path
    directory = f"/opt/airflow/tempSRCfiles/{folder_name.replace('default/', '')}"

    # Use os module to navigate and find the latest PLMXML file
    files = [f for f in os.listdir(directory) if f.endswith('.plmxml') and '_redirect' not in f]
    
    if files:
        # Sort files by modification time and get the latest one
        latest_file = sorted(files, key=lambda f: os.path.getmtime(os.path.join(directory, f)), reverse=True)[0]

        # Set Airflow variable 'plmxml_file' with the path to the latest file
        Variable.set('plmxml_file', latest_file)
        print(f"Set Airflow variable 'plmxml_file' to: {os.path.join(directory, latest_file)}")
    else:
        print("No suitable PLMXML file found.")

get_plmxml_structure_file = PythonOperator(
    task_id='process_latest_plmxml',
    python_callable=set_latest_plmxml,
    op_kwargs={'folder_name': '{{ var.value.current_space_id }}'},  # Use Airflow variable
    dag=dag,
)

# Subtasks 2: Download files
download_files = BashOperator(
    task_id='download_files',
    bash_command=(
        'python /opt/airflow/tempSRCfiles/download_task/download_ref_files.py '
        '/opt/airflow/tempSRCfiles/{{ var.value.current_space_id | replace("default/", "") }}/ '
        '{{ var.value.plmxml_file }}'
    ),
    dag=dag,
)

def trigger_step_convert(folder_name, file_name, **kwargs):
    try:
        # jt_files = []
        directory = '/opt/airflow/tempSRCfiles/' + folder_name.replace("default/", "") + '/' + file_name.replace(".plmxml", "_linked_files")

        for filename in os.listdir(directory):
            if filename.endswith(".jt"):
                jt_file = os.path.join(directory, filename)
                task_id = f'trigger_step_convert_{filename}'
                bash_command = (
                    'cd /opt/airflow/tempSRCfiles/coretech-2024-linux/build && '
                    './CoreTechEval '
                    f'{jt_file} '
                    f'{jt_file.replace(".jt", ".stp")}'
                )
                print(f"Processing bash_command: {bash_command}")
                trigger_step_convert_op = BashOperator(
                    task_id=task_id,
                    bash_command=bash_command,
                    env={'LD_LIBRARY_PATH': '/opt/airflow/tempSRCfiles/coretech-2024-linux/lib/core_tech/lib:$LD_LIBRARY_PATH'},
                    dag=kwargs['dag'],
                )
                trigger_step_convert_op.execute(context=kwargs)
        print("All JT files processed successfully.")
    except Exception as e:
        raise Exception(f"Error processing JT files: {e}")


trigger_step_convert_task = PythonOperator(
    task_id='trigger_step_convert_task',
    python_callable=trigger_step_convert,
    op_kwargs={'folder_name': '{{ var.value.current_space_id }}', 'file_name': '{{ var.value.plmxml_file }}'},
    provide_context=True,
    dag=dag,
)

# Subtasks 4: update_plmxml_stp_ref
update_plmxml_stp_ref = BashOperator(
    task_id='update_plmxml_stp_ref',
    bash_command=(
        'python /opt/airflow/tempSRCfiles/download_task/update_plmxml_stp_ref.py '
        '/opt/airflow/tempSRCfiles/{{ var.value.current_space_id | replace("default/", "") }}/ '
        '{{ var.value.plmxml_file | replace(".plmxml", "_redirect.plmxml")}}'
    ),
    dag=dag,
)

# Subtasks 3: Trigger STEP conversion
trigger_step_convert_plmxml_mono = BashOperator(
    task_id='trigger_step_convert_plmxml_mono',
    bash_command=(
            'cd /opt/airflow/tempSRCfiles/coretech-2024-linux/build && '
            './CoreTechEval '
            '/opt/airflow/tempSRCfiles/{{ var.value.current_space_id | replace("default/", "") }}/'
            '{{ var.value.plmxml_file | replace(".plmxml", "_redirect_stp.plmxml") }}   '
            '/opt/airflow/tempSRCfiles/{{ var.value.current_space_id | replace("default/", "") }}/'
            '{{ var.value.plmxml_file | replace(".plmxml", "_redirect_mono") }}.stp'
        ),    
    env={'LD_LIBRARY_PATH': '/opt/airflow/tempSRCfiles/coretech-2024-linux/lib/core_tech/lib:$LD_LIBRARY_PATH'},
    dag=dag,
)

# Final dummy task to signify the end of the DAG
end_of_dag = DummyOperator(
    task_id='end_of_dag',
    dag=dag,
)

# Set task dependencies without creating a cycle
trigger_export >> set_current_space_id >> create_global_folder_task >> nodejs_space_export_to_plxml >> get_plmxml_structure_file >> download_files >> trigger_step_convert_task >> update_plmxml_stp_ref >> trigger_step_convert_plmxml_mono >> end_of_dag
