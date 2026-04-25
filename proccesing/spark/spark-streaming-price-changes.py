from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, expr
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("polymarket-stream") \
    .getOrCreate()

# Schema
price_change_schema = StructType([
    StructField("asset_id", StringType()),
    StructField("price", StringType())
])

schema = StructType([
    StructField("market", StringType()),
    StructField("price_changes", ArrayType(price_change_schema)),
    StructField("timestamp", StringType())
])

# Read Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "websocket") \
    .load()

# Filtrar header
filtered = df.filter(
    expr("exists(headers, x -> x.key = 'event_type' AND cast(x.value as string) = 'price_change')")
)

# Parse JSON
parsed = filtered.select(
    from_json(col("value").cast("string"), schema).alias("data")
)

# Flatten
flat = parsed.select(
    explode(col("data.price_changes")).alias("pc"),
    col("data.timestamp").cast("long").alias("ts")
).select(
    col("pc.asset_id"),
    col("pc.price").cast("float").alias("price"),
    col("ts")
)

# Crear tabla temporal para los precios de los tokens
# CREATE TABLE outcome_tokens_updates (
#     asset_id TEXT,
#     price FLOAT,
#     ts BIGINT
# );
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/markets") \
        .option("dbtable", "outcome_tokens_updates") \
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