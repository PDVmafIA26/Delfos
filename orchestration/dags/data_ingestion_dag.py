from airflow.decorators import dag, task
from datetime import datetime
import subprocess
from scripts.data_ingestion import main


@dag(
    start_date=datetime(2026, 5, 10),
    schedule_interval="@daily",
    catchup=False,
    tags=["data_ingestion"]
)
def data_ingestion_dag():

    @task
    def run_data_ingestion():
        main()

    run_data_ingestion()


dag = data_ingestion_dag()