from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2

# 1. Spark Session
spark = SparkSession.builder \
    .appName("KafkaUserUpsert") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema JSON
position_schema = StructType([
    StructField("market_title", StringType()),
    StructField("outcome", StringType()),
    StructField("realized_pnl", DoubleType()),
    StructField("status", StringType())
])

wallet_schema = StructType([
    StructField("wallet_address", StringType()),
    StructField("profile", StructType([
        StructField("name", StringType())
    ])),
    StructField("trading", StructType([
        StructField("positions", ArrayType(position_schema))
    ]))
])

schema = StructType([
    StructField("metadata", MapType(StringType(), StringType())),
    StructField("wallets", ArrayType(wallet_schema))
])

# 3. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_info") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json")

# 4. Parse JSON
parsed = json_df.select(from_json(col("json"), schema).alias("data"))

wallets = parsed.select(explode(col("data.wallets")).alias("wallet"))

positions = wallets.select(
    col("wallet.wallet_address"),
    explode(col("wallet.trading.positions")).alias("pos")
)

# 5. Metrics
metrics = positions.select(
    col("wallet_address"),
    col("pos.realized_pnl").alias("pnl")
)

agg = metrics.groupBy("wallet_address").agg(
    sum(when(col("pnl") > 0, col("pnl")).otherwise(0)).alias("total_won"),
    sum(when(col("pnl") < 0, col("pnl")).otherwise(0)).alias("total_lost"),
    sum(col("pnl")).alias("net_pnl"),
    count("*").alias("total_positions")
)

# 6. Suspicious logic
suspect_percentage = 0.9
min_positions = 1
min_profit = 10000

result = agg.withColumn(
    "win_rate",
    when(col("total_positions") > 0,
        col("total_won") / col("total_positions")
    ).otherwise(0)
).withColumn(
    "es_sospechoso",
    (col("win_rate") >= suspect_percentage) &
    (col("total_positions") >= min_positions) &
    (col("total_won") >= min_profit)
).select(
    col("wallet_address"),
    col("total_won"),
    col("total_lost"),
    col("net_pnl"),
    col("total_positions").alias("total_position"),
    col("es_sospechoso")
)

result = result.filter(col("es_sospechoso") == True)

# 7. UPSERT to PostgreSQL
def upsert_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    batch_df.persist()

    url = "jdbc:postgresql://localhost:5432/markets"
    props = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    # Tabla staging (temporal)
    batch_df.write.jdbc(
        url=url,
        table="usuarios_staging",
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
    # CREATE TABLE usuarios_staging AS TABLE usuarios WITH NO DATA;
    cursor.execute("""
        INSERT INTO usuarios AS t (
            wallet_address, total_won, total_lost, net_pnl, total_position, es_sospechoso
        )
        SELECT 
            wallet_address, total_won, total_lost, net_pnl, total_position, es_sospechoso
        FROM usuarios_staging
        ON CONFLICT (wallet_address)
        DO UPDATE SET
            total_won = EXCLUDED.total_won,
            total_lost = EXCLUDED.total_lost,
            net_pnl = EXCLUDED.net_pnl,
            total_position = EXCLUDED.total_position,
            es_sospechoso = EXCLUDED.es_sospechoso;
    """)

    conn.commit()
    cursor.close()
    conn.close()

    batch_df.unpersist()

query = result.writeStream \
    .foreachBatch(upsert_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/users_upsert") \
    .start()

query.awaitTermination()