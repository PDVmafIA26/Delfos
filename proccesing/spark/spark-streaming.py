from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
import psycopg2

# SPARK SESSION
spark = SparkSession.builder \
    .appName("PolymarketStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# SCHEMA
schema = StructType([
    StructField("id", StringType(), True),
    StructField("markets", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("question", StringType(), True),
            StructField("description", StringType(), True),
            StructField("volume", StringType(), True),
            StructField("category", StringType(), True),
            StructField("liquidity", StringType(), True),
            StructField("outcomePrices", StringType(), True)
        ])
    ), True)
])

# KAFKA SOURCE
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING) as json")

df = df.select(from_json(col("json"), schema).alias("data"))

# EXPLODE MARKETS
df = df.selectExpr("data.id as event_id", "explode(data.markets) as market")

df = df.select(
    col("market.id").alias("market_id"),
    col("market.question"),
    col("market.description"),
    col("market.volume"),
    col("market.liquidity"),
    col("market.outcomePrices")
)

# CLEANING
df_clean = df \
    .withColumn("volume", col("volume").cast("double")) \
    .withColumn("liquidity", col("liquidity").cast("double")) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("prices_array", from_json(col("outcomePrices"), ArrayType(StringType()))) \
    .withColumn("price", col("prices_array")[0].cast("double")) \
    .filter(col("volume").isNotNull()) \
    .filter(col("price").isNotNull())

# POSTGRES WRITER (RAW DATA)
def write_to_postgres(batch_df, batch_id):
    print(f"Writing batch {batch_id}")

    if batch_df.isEmpty():
        return

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    for row in batch_df.collect():

        cursor.execute("""
            INSERT INTO market_stream (
                market_id,
                question,
                description,
                volume,
                liquidity,
                price,
                timestamp
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (market_id)
            DO UPDATE SET
                volume = EXCLUDED.volume,
                liquidity = EXCLUDED.liquidity,
                price = EXCLUDED.price,
                timestamp = EXCLUDED.timestamp
        """, (
            row.market_id,
            row.question,
            row.description,
            row.volume,
            row.liquidity,
            row.price,
            row.timestamp
        ))

    conn.commit()
    cursor.close()
    conn.close()

# STREAM START
query = df_clean.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

print("Streaming started...")

query.awaitTermination()