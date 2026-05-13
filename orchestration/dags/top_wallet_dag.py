from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from scripts.top_wallets_processor import run_top_wallets_ingestion


@dag(
    start_date=datetime(2026, 5, 10),
    schedule="0 */8 * * *",  # Cada 8 horas
    catchup=False,
    tags=["top-wallets", "ingestion"],
)
def top_wallets_ingestion_dag():

    @task
    def clear_top_wallets():
        hook = PostgresHook(postgres_conn_id="polymarket")

        hook.run("""
            DELETE FROM top_wallets;
        """)

    @task
    def fetch_conditions_id():
        
        hook = PostgresHook(postgres_conn_id="polymarket")
        records = hook.get_records("""
            SELECT condition_id
            FROM mercados_master;
        """)
        
        return [r[0] for r in records]

    @task
    def process_top_wallets(conditions_id):
        http_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        http_session.mount("https://", adapter)
        http_session.mount("http://", adapter)
        MAX_WORKERS = 20
        run_top_wallets_ingestion(http_session, conditions_id, MAX_WORKERS)

    clear = clear_top_wallets()
    conditions_id = fetch_conditions_id()
    process = process_top_wallets(conditions_id)

    # Orden de ejecución
    clear >> conditions_id >> process


dag = top_wallets_ingestion_dag()