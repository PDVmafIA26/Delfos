from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2

# 1. Spark Session
spark = SparkSession.builder \
    .appName("KafkaTopWalletUpsert") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema JSON — estructura real del canal user de Polymarket WebSocket
# Los datos llegan tal cual desde la API, sin transformación en ingesta.
maker_order_schema = StructType([
    StructField("asset_id",       StringType()),
    StructField("matched_amount", StringType()),
    StructField("order_id",       StringType()),
    StructField("outcome",        StringType()),
    StructField("owner",          StringType()),
    StructField("price",          StringType()),
])

schema = StructType([
    StructField("asset_id",       StringType()),
    StructField("event_type",     StringType()),
    StructField("id",             StringType()),
    StructField("last_update",    StringType()),
    StructField("maker_orders",   ArrayType(maker_order_schema)),
    StructField("market",         StringType()),    
    StructField("matchtime",      StringType()),
    StructField("outcome",        StringType()),
    StructField("owner",          StringType()),    
    StructField("price",          StringType()),
    StructField("side",           StringType()),    
    StructField("size",           StringType()),   
    StructField("status",         StringType()),    
    StructField("taker_order_id", StringType()),
    StructField("timestamp",      StringType()),
    StructField("trade_owner",    StringType()),   # !!!! pendiente confirmar si es wallet o UUID
    StructField("type",           StringType()),    
])

# 3. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "top_wallets") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json")

# 4. Parse JSON y filtrar solo eventos de tipo TRADE MATCHED
parsed = json_df.select(from_json(col("json"), schema).alias("data"))


# 5. Extraer campos que corresponden a la tabla TOP_WALLETS
result = parsed.select(
    col("data.trade_owner").alias("wallet_address"),
    col("data.market").alias("conditionId"),
    col("data.asset_id"),
    col("data.size").cast(DoubleType()).alias("amount")
)

# 6. UPSERT to PostgreSQL
def upsert_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
 
    batch_df.persist()
 
    url = "jdbc:postgresql://localhost:5432/markets"
    props = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
 
    # Tabla staging (temporal)
    # CREATE TABLE top_wallets_staging AS TABLE top_wallets WITH NO DATA;
    batch_df.write.jdbc(
        url=url,
        table="top_wallets_staging",
        mode="overwrite",
        properties=props
    )
 
    # Ejecutar UPSERT
    conn = psycopg2.connect(
        dbname="markets",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
 
    cursor = conn.cursor()
 
    cursor.execute("""
        INSERT INTO top_wallets (wallet_address, "conditionId", asset_id, amount)
        SELECT wallet_address, "conditionId", asset_id, amount
        FROM top_wallets_staging
        ON CONFLICT (wallet_address, "conditionId")
        DO UPDATE SET
            asset_id = EXCLUDED.asset_id,
            amount   = top_wallets.amount + EXCLUDED.amount
    """)
 
    conn.commit()
    cursor.close()
    conn.close()
 
    batch_df.unpersist()
 
query = result.writeStream \
    .foreachBatch(upsert_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/top_wallets_upsert") \
    .start()
 
query.awaitTermination()