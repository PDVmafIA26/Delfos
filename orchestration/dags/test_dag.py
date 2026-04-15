from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 14),
}

with DAG(
    dag_id='test_python_script_dag',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # cada 5 minutos
    catchup=False,
) as dag:

    run_script = BashOperator(
        task_id='run_test_script',
        bash_command='python /opt/airflow/scripts/test_script.py'
    )

    run_script