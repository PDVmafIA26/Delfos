from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, expr
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("polymarket-stream-flip") \
    .getOrCreate()

# Schema
schema = StructType([
    StructField("market", StringType()),
    StructField("asset_id", StringType()),
    StructField("price", StringType()),
    StructField("size", StringType()),
    StructField("fee_rate_bps", StringType()),
    StructField("side", StringType()),
    StructField("timestamp", StringType()),
    StructField("event_type", StringType()),
    StructField("transaction_hash", StringType())
])

# Read Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "websocket") \
    .load()

# Filter header
filtered = df.filter(
    expr("exists(headers, x -> x.key = 'event_type' AND cast(x.value as string) = 'last_trade_price')")
)

# Parse JSON
parsed = filtered.select(
    from_json(col("value").cast("string"), schema).alias("data")
)

# Filter only BUY
parsed = parsed.filter(col("data.side") == "BUY")

# Flatten
flat = parsed.select(
    col("data.asset_id").alias("asset_id"),
    col("data.price").cast("float").alias("price"),
    col("data.size").cast("float").alias("size")
)

def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/markets") \
        .option("dbtable", "last_trade_price") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = flat.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

query.awaitTermination()