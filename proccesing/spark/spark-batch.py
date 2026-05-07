from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Spark session for MongoDB
spark = SparkSession.builder \
    .appName("PolymarketBatchMongo") \
    .config("spark.mongodb.read.connection.uri", "mongodb://admin:administrador@mongodb:27017/markets.tech_markets") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema (opcional pero recomendable)
schema = StructType([
    StructField("market_id", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("question", StringType(), True),
    StructField("volume", StringType(), True)
])

# Read data from MongoDB
df = spark.read \
    .format("mongodb") \
    .load()

# Transform fields
df = df \
    .withColumn("volume", col("volume").cast("double")) \
    .withColumn("timestamp", to_timestamp(col("ingestion_timestamp")))

# Compute stats
stats = df.groupBy("market_id").agg(
    avg("volume").alias("mean_volume"),
    stddev("volume").alias("std_volume"),
    count("*").alias("num_records")
)

# Write to PostgreSQL
stats.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/markets") \
    .option("dbtable", "market_stats") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Batch job completed: data read from MongoDB and saved in PostgreSQL.")