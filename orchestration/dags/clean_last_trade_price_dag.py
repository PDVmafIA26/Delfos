from airflow.decorators import dag, task
from datetime import datetime
import psycopg2

@dag(
    start_date=datetime(2026, 5, 10),
    schedule_interval="@daily",
    catchup=False,
    tags=["clean_last_trade_price_condition_id", "data_cleaning"],
)
def clean_last_trade_price_dag():

    @task
    def clean_last_trade_price():
        conn = psycopg2.connect(
                dbname="markets",
                user="postgrs",
                password="postgres",
                host="localhost",
                port="5432"
            )
            
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM last_trade_price;")
            conn.commit()
            print("Tabla limpiada correctamente.")
        except Exception as e:
            conn.rollback()
            print("Error:", e)
        finally:
            cursor.close()
        conn.close()
        
    clean_last_trade_price()

dag = clean_last_trade_price_dag()