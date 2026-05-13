from airflow.decorators import dag, task
from datetime import datetime
import subprocess


@dag(
    start_date=datetime(2026, 5, 10),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["data_ingestion"]
)
def market_dag():

    @task
    def run_markets():
        script_path = "/opt/airflow/scripts/markets.py"

        result = subprocess.run(
            ["python", script_path],
            capture_output=True,
            text=True)

        if result.stderr:
            print("STDERR:")
            print(result.stderr)

        if result.returncode != 0:
            raise Exception(
                f"Script failed with return code {result.returncode}"
            )

    run_markets()


dag = market_dag()