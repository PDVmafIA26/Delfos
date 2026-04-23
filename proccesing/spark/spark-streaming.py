from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import uuid
import json
from datetime import datetime, timezone

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
    StructField("outcomePrices", StringType(), True),
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
    .withColumn("prices_array", from_json(col("outcomePrices"), ArrayType(StringType()))) \
    .withColumn("price", col("prices_array")[0].cast("double")) \
    .filter(col("volume").isNotNull()) \
    .filter(col("timestamp").isNotNull())

# # WINDOWING
# windowed = df_clean \
#     .withWatermark("timestamp", "10 minutes") \
#     .groupBy(
#         window(col("timestamp"), "5 minutes"), # Analyze data from the last 5 minutes
#         col("market_id")
#     ).agg(
#         avg("volume").alias("avg_volume"),
#         max("volume").alias("max_volume")
#     )


# # FLATTEN
# anomalies = anomalies.select(
#     col("market_id"),
#     col("window.start").alias("window_start"),
#     col("window.end").alias("window_end"),
#     col("avg_volume"),
#     col("max_volume"),
#     #col("is_anomaly")
# )

def detect_volume_peaks(batch_df):
    df_volume = batch_df.groupBy(
        "market_id",
        window(col("timestamp"), "5 minutes")
    ).agg(
        sum("volume").alias("stream_volume_5min")
    )

    rows = df_volume.collect()

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    for row in rows:
        market_id = row.market_id
        stream_volume = row.stream_volume_5min

        # 1. baseline histórico
        cursor.execute("""
            SELECT historical_vol_5min_avg
            FROM mercados_master
            WHERE market_id = %s
        """, (market_id,))

        result = cursor.fetchone()
        historical_avg = result[0] if result else None

        if historical_avg is None or historical_avg == 0:
            continue

        ratio = stream_volume / historical_avg

        if ratio > 5:
            if ratio > 10:
                severity = "CRITICAL"
            elif ratio > 7:
                severity = "HIGH"
            else:
                severity = "MEDIUM"

            payload = {
                "ratio": float(ratio),
                "usd_detected": float(stream_volume),
                "severity": severity
            }

            cursor.execute("""
                INSERT INTO alertas_anomalias (
                    category,
                    sub_type,
                    payload,
                    created_at
                )
                VALUES (%s, %s, %s, %s)
            """, (
                "MARKET",
                "VOLUME_SPIKE_VS_HISTORY",
                json.dumps(payload),
                datetime.now(timezone.utc)
            ))
    conn.commit()
    cursor.close()
    conn.close()

def detect_price_anomalies(batch_df):

    rows = batch_df.collect()

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    for row in rows:
        market_id = row.market_id
        current_price = row.price
        timestamp = row.timestamp
        outcome_name = "Yes"

        # 1. Obtener precio anterior desde BD
        cursor.execute("""
            SELECT current_price 
            FROM outcome_tokens
            WHERE market_id = %s AND outcome_name = %s
            LIMIT 1
        """, (market_id, outcome_name))

        result = cursor.fetchone()

        prev_price = result[0] if result else None

        is_anomaly = False
        change=""
        # 2. Detectar cruce
        if prev_price is not None:
                if prev_price < 0.5 and current_price >= 0.5:
                    is_anomaly = True
                    change = "NO_TO_YES"
                elif prev_price >= 0.5 and current_price < 0.5:
                    is_anomaly = True
                    change = "YES_TO_NO"

        # 3. Guardar anomalía
        if is_anomaly:
            payload = {
                "change": change,
                "actual_price": float(current_price),
                "previous_price": float(prev_price),
                "market_id": market_id
            }
            
            cursor.execute("""
                INSERT INTO anomalies (
                    category,
                    sub_type,
                    payload,
                    created_at
                )
                VALUES (%s, %s, %s, %s)
            """, (
                "MARKET",
                "PRICE_CROSS_0_5",
                json.dumps(payload),
                datetime.now(timezone.utc)
            ))

        # 4. Actualizar precio en outcome_tokens
        cursor.execute("""
            UPDATE outcome_tokens
            SET current_price = %s
            WHERE market_id = %s AND outcome_name = %s
        """, (current_price, market_id, outcome_name))

    conn.commit()
    cursor.close()
    conn.close()

def detect_price_change(batch_df):

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    rows = batch_df.collect()

    for row in rows:
        market_id = row.market_id
        current_price = row.price

        # 1. obtener last stable price
        cursor.execute("""
            SELECT last_stable_price
            FROM mercados_master
            WHERE market_id = %s
        """, (market_id,))

        result = cursor.fetchone()
        last_stable = result[0] if result else None

        if last_stable is None or last_stable == 0:
            continue

        # 2. delta %
        delta_pct = ((current_price - last_stable) / last_stable) * 100

        is_anomaly = abs(delta_pct) > 20

        if is_anomaly:

            payload = {
                "variation_pct": float(delta_pct),
                "is_pre_flip": True if abs(delta_pct) > 50 else False
            }

            cursor.execute("""
                INSERT INTO alertas_anomalias (
                    category,
                    sub_type,
                    payload,
                    created_at
                )
                VALUES (%s, %s, %s, %s)
            """, (
                "MARKET",
                "PRICE_VARIATION_20PCT",
                json.dumps(payload),
                datetime.now(timezone.utc)
            ))

        # 3. actualizar stable price (MUY IMPORTANTE)
        cursor.execute("""
            UPDATE mercados_master
            SET last_stable_price = %s
            WHERE market_id = %s
        """, (
            current_price,
            market_id
        ))

    conn.commit()
    cursor.close()
    conn.close()

def process_batch(batch_df, batch_id):
    print(f"\nPROCESSING BATCH {batch_id}")

    if batch_df.isEmpty():
        return

    # 1. PRICE ANOMALY (stream_df)
    detect_price_anomalies(batch_df)

    # 2. VOLUME ANOMALY (recalculated here)
    detect_volume_peaks(batch_df)

    # 3. changes in the exchange rate
    detect_price_change(batch_df)

# STREAM FINAL
query = df_clean.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()


print("Streaming started...")

query.awaitTermination()