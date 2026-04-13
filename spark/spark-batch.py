from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("PolymarketBatch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming Kafka JSON messages
schema = StructType([
    StructField("market_id", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("question", StringType(), True),
    StructField("volume", StringType(), True)
])

# Read raw data from Kafka topic
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "tech_markets") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast Kafka value to string and parse JSON
df = df_raw.selectExpr("CAST(value AS STRING) as json")
df = df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# Transform fields
df = df \
    .withColumn("volume", col("volume").cast("double")) \
    .withColumn("timestamp", to_timestamp(col("ingestion_timestamp")))

# Compute historical statistics per market and question
stats = df.groupBy("market_id").agg(
    avg("volume").alias("mean_volume"),
    stddev("volume").alias("std_volume"),
    count("*").alias("num_records")
)

# Write results to PostgreSQL
stats.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/markets") \
    .option("dbtable", "market_stats") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Batch job completed and stored in PostgreSQL.")