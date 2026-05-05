import psycopg2

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