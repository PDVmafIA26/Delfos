from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta


# DAG: DELFOS DAILY GOLD STATS
# Executes batch functions every 24 hours

@dag(
    start_date=datetime(2026, 5, 15),
    schedule_interval="@daily",
    catchup=False
)
def delfos_daily_gold_stats_dag():

    # TASK 1: REFRESH MARKET DAILY STATS

    @task
    def refresh_market_daily_stats():

        hook = PostgresHook(postgres_conn_id="polymarket")

        hook.run("""
            SELECT refresh_market_daily_stats();
        """)

        print("market_daily_stats actualizado correctamente")


    # TASK 2: REFRESH USER STATS BATCH

    @task
    def refresh_user_stats_batch():

        hook = PostgresHook(postgres_conn_id="polymarket")

        hook.run("""
            SELECT refresh_user_stats_batch();
        """)

        print("user_stats_batch actualizado correctamente")


    # Orchestation

    market_stats = refresh_market_daily_stats()
    user_stats = refresh_user_stats_batch()

    market_stats >> user_stats


# Final instance of the DAG
dag = delfos_daily_gold_stats_dag()