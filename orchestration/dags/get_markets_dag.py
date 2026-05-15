from airflow.decorators import dag, task
from datetime import datetime
import subprocess
from scripts.markets import main


@dag(
    start_date=datetime(2026, 5, 10),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["markets"]
)
def market_dag():

    @task
    def run_markets():
        main()

    run_markets()


dag = market_dag()