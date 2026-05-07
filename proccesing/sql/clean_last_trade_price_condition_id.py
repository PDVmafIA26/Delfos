import psycopg2

# Esto habría que llamarlo cada vez que se detecte un flip. De esta froma no se generan anomalías duplicadas
def delete_last_trade_by_condition(condition_id):
    conn = psycopg2.connect(
        dbname="markets",
        user="postgrs",
        password="postgres",
        host="localhost",
        port="5432"
    )
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Esto borra todos los trades según el condition id de un mercado, manteniendo el último registro para poder
                # compararlo cuando venga un trade price nuevo
                query = """
                DELETE FROM last_trade_price
                WHERE idautoincremental IN (
                    SELECT idautoincremental
                    FROM (
                        SELECT ltp.idautoincremental,
                            ROW_NUMBER() OVER (
                                PARTITION BY ltp.asset_id
                                ORDER BY ltp.idautoincremental DESC
                            ) as rn
                        FROM last_trade_price ltp
                        JOIN outcome_tokens ot
                        ON ltp.asset_id = ot.asset_id
                        WHERE ot.condition_id = %s
                    ) sub
                    WHERE rn > 1
                );
                """
                cur.execute(query, (condition_id,))
                
    finally:
        conn.close()