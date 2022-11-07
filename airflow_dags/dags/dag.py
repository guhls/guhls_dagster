#!/env/bin python3

from airflow.models import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
from func import get_columns, get_data_json


dag = DAG(dag_id='kingston_dag', start_date=dt.datetime(2022, 10, 5), schedule_interval="0 * * * *")

endpoint = 'https://opendatakingston.cityofkingston.ca/api/records/1.0/search/?dataset=capital-planning-lines'


get_json = PythonOperator(
    dag=dag,
    task_id='get_json_task',
    python_callable=get_data_json,
    op_kwargs={'url': endpoint}
)

json = get_json.output

get_columns = PythonOperator(
    dag=dag,
    task_id='get_column_task',
    python_callable=get_columns,
    op_kwargs={'json': json}
)
