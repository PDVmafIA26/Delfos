from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    start_date=datetime(2026, 5, 10),
    schedule_interval="@daily",
    catchup=False,
    tags=["clean_last_trade_price_condition_id", "data_cleaning"],
)
def clean_last_trade_price_dag():

    @task
    def clean_last_trade_price():
        hook = PostgresHook(postgres_conn_id="polymarket")

        hook.run("""
            DELETE FROM top_wallets;
        """)
        
    clean_last_trade_price()

dag = clean_last_trade_price_dag()