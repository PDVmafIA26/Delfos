from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# SPARK SESSION
spark = SparkSession.builder \
    .appName("PolymarketStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# SCHEMA
schema = StructType([
    StructField("market_id", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("question", StringType(), True),
    StructField("description", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("liquidity", StringType(), True),
    StructField("outcome_prices", StringType(), True),
    StructField("outcomes", StringType(), True),
    StructField("order_book", StringType(), True)
])

# We connect to kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "tech_markets") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df = df_raw.selectExpr("CAST(value AS STRING) as json")

df = df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# We clean the data
df_clean = df \
    .withColumn("volume", col("volume").cast("double")) \
    .withColumn("liquidity", col("liquidity").cast("double")) \
    .withColumn("timestamp", to_timestamp(col("ingestion_timestamp"))) \
    .filter(col("volume").isNotNull()) \
    .filter(col("timestamp").isNotNull())

# WINDOWING
windowed = df_clean \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"), # Analyze data from the last 5 minutes
        col("market_id")
    ).agg(
        avg("volume").alias("avg_volume"),
        max("volume").alias("max_volume")
    )

# ANOMALY DETECTION
anomalies = windowed \
    .withColumn(
        "is_anomaly",
        (col("max_volume") > col("avg_volume") * 5)
    ) #\
    #.filter(col("is_anomaly") == True)

# FLATTEN
anomalies = anomalies.select(
    col("market_id"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_volume"),
    col("max_volume"),
    col("is_anomaly")
)

# POSTGRES WRITER
def write_to_postgres(batch_df, batch_id):
    print(f"\nPROCESSING BATCH {batch_id}") # Debug
    print(f"Rows: {batch_df.count()}")

    if batch_df.count() > 0:
        batch_df.show(truncate=False)

        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/markets") \
            .option("dbtable", "anomalies") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

# STREAM FINAL
query = anomalies.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

print("Streaming started...")

query.awaitTermination()