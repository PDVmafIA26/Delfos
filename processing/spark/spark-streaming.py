from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import execute_batch

# Spark Session
spark = SparkSession.builder \
    .appName("PolymarketStreaming") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "2") \
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
            StructField("outcomePrices", StringType(), True),
            StructField("clobTokenIds", StringType(), True)
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

# Write function
def write_to_postgres(batch_df, batch_id):
    print(f"Batch {batch_id}")

    if batch_df.isEmpty():
        return

    print(f"Batch {batch_id}")

    # Explode markets
    df = batch_df.withColumn("market", explode("markets"))

    # Events
    events_df = df.select(
        col("id").alias("event_id"),
        col("title").alias("question"),
        col("slug"),
        col("image"),
        col("active"),
        col("liquidity"),
        col("volume"),
        col("volume24hr").alias("volume_24hr"),
        col("volume1wk").alias("volume_1w"),
        col("volume1mo").alias("volume_1mo"),
        col("volume1yr").alias("volume_1yr")
    ).dropDuplicates(["event_id"])

    # Markets
    markets_df = df.select(
        col("id").alias("event_id"),
        col("market.id").alias("id"),
        col("market.conditionId").alias("condition_id"),
        col("market.slug"),
        col("market.question"),
        col("market.image"),
        col("market.liquidity").cast("double").alias("liquidity"),
        col("market.volume").cast("double").alias("volume"),
        col("market.volume24hr").alias("volume_24hr"),
        col("market.volume1wk").alias("volume_1w"),
        col("market.volume1mo").alias("volume_1mo"),
        col("market.volume1yr").alias("volume_1yr")
    )

    markets_df = markets_df.filter(col("condition_id").isNotNull() & (col("condition_id") != ""))

    # Outcomes
    outcomes_df = df \
        .withColumn("outcomes_array", from_json(col("market.outcomes"), ArrayType(StringType()))) \
        .withColumn("prices_array", from_json(col("market.outcomePrices"), ArrayType(StringType()))) \
        .withColumn("tokens_array", from_json(col("market.clobTokenIds"), ArrayType(StringType()))) \
        .withColumn("zipped", arrays_zip("outcomes_array", "prices_array", "tokens_array")) \
        .withColumn("exploded", explode("zipped")) \
    .select(
        col("market.conditionId").alias("condition_id"),
        col("exploded.outcomes_array").alias("outcome_name"),
        col("exploded.prices_array").cast("double").alias("price"),
        col("exploded.tokens_array").alias("asset_id")
    )

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    # inserts...

    # Events
    events = events_df.collect()
    execute_batch(cursor, """
        INSERT INTO eventos (
            event_id, question, slug, image, active,
            liquidity, volume, volume_24h, volume_1w, volume_1mo, volume_1yr
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (event_id) DO UPDATE SET
            question = EXCLUDED.question,
            liquidity = EXCLUDED.liquidity,
            volume = EXCLUDED.volume
    """, [
        (
            r.event_id, r.question, r.slug, r.image, r.active,
            r.liquidity, r.volume, r.volume_24hr,
            r.volume_1w, r.volume_1mo, r.volume_1yr
        ) for r in events
    ])


    markets = markets_df.collect()
    execute_batch(cursor, """
        INSERT INTO mercados_master (
            id, condition_id, slug, question, image,
            liquidity, volume, volume_24h, volume_1w, volume_1mo, volume_1yr, event_id
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id, condition_id) DO UPDATE SET
            volume = EXCLUDED.volume,
            liquidity = EXCLUDED.liquidity
    """, [
        (
            r.id, r.condition_id, r.slug, r.question, r.image,
            r.liquidity, r.volume, r.volume_24hr,
            r.volume_1w, r.volume_1mo, r.volume_1yr, r.event_id
        ) for r in markets
    ])

    # outcome tokens
    outcomes = outcomes_df.collect()
    execute_batch(cursor, """
        INSERT INTO outcome_tokens (
            asset_id, condition_id, outcome_name, price
        )
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (asset_id) DO UPDATE SET
            price = EXCLUDED.price
    """, [
        (
            r.asset_id,
            r.condition_id,
            r.outcome_name,
            r.price
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