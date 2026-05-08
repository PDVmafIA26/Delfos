from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import execute_batch

# Spark Session
spark = SparkSession.builder \
    .appName("PolymarketStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("image", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("liquidity", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("volume24hr", DoubleType(), True),
    StructField("volume1wk", DoubleType(), True),
    StructField("volume1mo", DoubleType(), True),
    StructField("volume1yr", DoubleType(), True),
    StructField("markets", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("conditionId", StringType(), True),
            StructField("slug", StringType(), True),
            StructField("question", StringType(), True),
            StructField("image", StringType(), True),
            StructField("liquidity", StringType(), True),
            StructField("volume", StringType(), True),
            StructField("volume24hr", DoubleType(), True),
            StructField("volume1wk", DoubleType(), True),
            StructField("volume1mo", DoubleType(), True),
            StructField("volume1yr", DoubleType(), True),
            StructField("outcomes", StringType(), True),
            StructField("outcomePrices", StringType(), True)
        ])
    ), True)
])

# Kafka source
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 50) \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING) as json")

df = df.select(from_json(col("json"), schema).alias("data"))

# Explode Markets
df = df.select(col("data.*")).withColumn("market", explode("markets"))

# events
events_df = df.select(
    col("id").alias("eventid"),
    col("title").alias("question"),
    col("slug"),
    col("image"),
    col("active"),
    col("liquidity"),
    col("volume"),
    col("volume24hr"),
    col("volume1wk"),
    col("volume1mo"),
    col("volume1yr")
).dropDuplicates(["eventid"])

# markets
markets_df = df.select(
    col("id").alias("eventid"),
    col("market.id").alias("id"),
    col("market.conditionId").alias("conditionid"),
    col("market.slug"),
    col("market.question"),
    col("market.image"),
    col("market.liquidity").cast("double"),
    col("market.volume").cast("double"),
    col("market.volume24hr"),
    col("market.volume1wk"),
    col("market.volume1mo"),
    col("market.volume1yr")
)

# Outcome Tokens
outcomes_df = df \
    .withColumn("outcomes_array", from_json(col("market.outcomes"), ArrayType(StringType()))) \
    .withColumn("prices_array", from_json(col("market.outcomePrices"), ArrayType(StringType()))) \
    .withColumn("zipped", arrays_zip("outcomes_array", "prices_array")) \
    .withColumn("exploded", explode("zipped")) \
    .select(
        col("market.conditionId").alias("condition_id"),
        col("exploded.outcomes_array").alias("outcome_name"),
        col("exploded.prices_array").cast("double").alias("price")
    )

# Write function
def write_to_postgres(batch_df, batch_id):
    print(f"Batch {batch_id}")

    if batch_df.isEmpty():
        return

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )
    cursor = conn.cursor()

    # Events
    events = events_df.collect()
    execute_batch(cursor, """
        INSERT INTO eventos (
            eventid, question, slug, image, active,
            liquidity, volume, volume24h, volume1w, volume1mo, volume1yr
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (eventid) DO UPDATE SET
            question = EXCLUDED.question,
            liquidity = EXCLUDED.liquidity,
            volume = EXCLUDED.volume
    """, [
        (
            r.eventid, r.question, r.slug, r.image, r.active,
            r.liquidity, r.volume, r.volume24hr,
            r.volume1wk, r.volume1mo, r.volume1yr
        ) for r in events
    ])

    # Markets
    markets = markets_df.collect()
    execute_batch(cursor, """
        INSERT INTO mercados_master (
            id, conditionid, slug, question, image,
            liquidity, volume, volume24h, volume1w, volume1mo, volume1yr, eventid
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            volume = EXCLUDED.volume,
            liquidity = EXCLUDED.liquidity
    """, [
        (
            r.id, r.conditionid, r.slug, r.question, r.image,
            r.liquidity, r.volume, r.volume24hr,
            r.volume1wk, r.volume1mo, r.volume1yr, r.eventid
        ) for r in markets
    ])

    # outcome tokens
    outcomes = outcomes_df.collect()
    execute_batch(cursor, """
        INSERT INTO outcome_tokens (
            condition_id, outcome_name, price
        )
        VALUES (%s,%s,%s)
    """, [
        (
            r.condition_id, r.outcome_name, r.price
        ) for r in outcomes
    ])

    conn.commit()
    cursor.close()
    conn.close()

# Stream Start
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()


print("Streaming started...")

query.awaitTermination()