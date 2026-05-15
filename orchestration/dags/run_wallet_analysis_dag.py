import requests
from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scripts.wallet_analyzer import run_wallet_analysis_pipeline

@dag(
    start_date=datetime(2026, 5, 10),
    schedule="0 */6 * * *",  # Cada 6 horas
    catchup=False,
    tags=["data_ingestion", "wallet_analysis"],
)
def user_analysis_dag():
    
    @task
    def load_wallets():
        
        hook = PostgresHook(postgres_conn_id="polymarket")

        records = hook.get_records("""
            SELECT wallet_address FROM usuarios;
        """)
    
        return [r[0] for r in records]
    
    @task
    def run_wallet_analysis(wallets):
        http_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        http_session.mount("https://", adapter)
        http_session.mount("http://", adapter)

        MAX_WORKERS = 20
        run_wallet_analysis_pipeline(http_session, wallets, max_workers=MAX_WORKERS)


    wallets = load_wallets()
    users = run_wallet_analysis(wallets)

    wallets >> users


dag = user_analysis_dag()