from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a Spark session with configuration for MinIO (S3)
spark = SparkSession.builder \
    .appName("PolymarketBatchMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "administrador") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming Kafka JSON messages
schema = StructType([
    StructField("market_id", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("question", StringType(), True),
    StructField("volume", StringType(), True)
])

# Read raw data from MinIO
df = spark.read.schema(schema).json("s3a://nombre-bucket/")

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

print("Batch job completed: data read from MinIO and saved in PostgreSQL.")