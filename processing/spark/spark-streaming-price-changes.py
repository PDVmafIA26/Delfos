from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, expr
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("polymarket-stream-flip") \
    .getOrCreate()

# Schema
price_change_schema = StructType([
    StructField("asset_id", StringType()),
    StructField("price", StringType()),
    StructField("size", StringType()),
    StructField("side", StringType()),
    StructField("hash", StringType()),
    StructField("best_bid", StringType()),
    StructField("best_ask", StringType())
])

# Schema principal
schema = StructType([
    StructField("market", StringType()),
    StructField("price_changes", ArrayType(price_change_schema)),
    StructField("timestamp", StringType()),
    StructField("event_type", StringType())
])

# Read Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "websockets") \
    .load()

# Filter header
filtered = df.filter(col("key").cast("string") == "price_change")

# Parse JSON
parsed = filtered.select(from_json(col("value").cast("string"), schema).alias("data"))

# Explode array
exploded = parsed.select(
    col("data.market").alias("market"),
    col("data.timestamp").alias("timestamp"),
    explode(col("data.price_changes")).alias("change")
)

# Filter only BUY
filtered = exploded.filter(
    col("change.side") == "BUY"
)

# Flatten
flat = filtered.select(
    col("change.asset_id").alias("asset_id"),
    col("change.price").cast("float").alias("price"),
    col("change.size").cast("float").alias("size")
)

def write_to_postgres(batch_df, batch_id):
    print(f"Batch {batch_id}")
    try:
        batch_df.show(truncate=False)
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/markets") \
            .option("dbtable", "last_trade_price") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"An error has occurred: {e}")

query = flat.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

query.awaitTermination()