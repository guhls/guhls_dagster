from airflow.models import DAG
from airflow.operators.python import PythonOperator
from dags_airflow import etl_pipe
import datetime

dag = DAG(
    dag_id="etl_pipe",
    schedule="0 * * * *",
    start_date=datetime.datetime(2022, 10, 11)
)

simple_etl = PythonOperator(
    task_id="simple_etl",
    python_callable=etl_pipe,
    dag=dag,
)
