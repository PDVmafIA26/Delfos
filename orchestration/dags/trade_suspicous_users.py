from airflow.decorators import dag, task
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scripts.wallet_current_positions import analyze_multiple_wallets_positions
from scripts.kafka_managerV2 import get_producer
from datetime import datetime

@dag(start_date=datetime(2026, 5, 10), schedule_interval="*/5 * * * *", catchup=False) # Cada tres horas se ejecutaría 'wallet_analysis_dag'
def wallet_analysis_dag():

    @task
    def fetch_wallets():
        
        hook = PostgresHook(postgres_conn_id="polymarket")
        records = hook.get_records("""
            SELECT wallet_address
            FROM usuarios
            WHERE es_sospechoso = true;
        """)
        
        return [r[0] for r in records]

    @task
    def process_wallets(wallet_addresses):
        http_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        http_session.mount("https://", adapter)
        http_session.mount("http://", adapter)
        MAX_WORKERS = 20
        analyze_multiple_wallets_positions(http_session, wallet_addresses, max_workers=MAX_WORKERS)
        if get_producer():
            get_producer().flush()
            print("\n[✓] All raw messages successfully flushed to Kafka.")

    wallets = fetch_wallets()
    process_wallets(wallets)

dag = wallet_analysis_dag()