from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.dag import DAG
import datetime
from pathlib import Path
import os

root_path = Path(__file__).parent.parent.parent
list_files = os.listdir(f"{root_path}/bin/")

default_args = {
    'start_date': datetime.datetime.today(),
}

dag = DAG(default_args=default_args, dag_id='dag_count_lines_in_csv')

task_detect_file = FileSensor(
    filepath=f'{root_path}/bin/201812SpotifyData.csv',
    mode='poke',
    poke_interval=5,
    timeout=15,
    task_id="detect_file",
    dag=dag)

task_count_lines = BashOperator(
    task_id='count_lines',
    dag=dag,
    bash_command=f"for file in {root_path}/bin/*.csv;do wc -l $file; done",
)

task_detect_file >> task_count_lines
