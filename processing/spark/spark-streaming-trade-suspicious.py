from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)
from pyspark.sql.functions import (
    col, from_json, explode, lit, when, to_date, current_date
)
from datetime import datetime
import requests
import psycopg2

# 1. Spark session
spark = SparkSession.builder \
    .appName("Kafka_to_Trade_Sospechosos") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. FIXED SCHEMA (ARRAY OF POSITIONS)
schema = ArrayType(StructType([
    StructField("proxyWallet", StringType()),
    StructField("asset", StringType()),
    StructField("size", DoubleType()),
    StructField("title", StringType()),
    StructField("realizedPnl", DoubleType()),
    StructField("endDate", StringType())
]))

# 3. Read from Kafka
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "wallet_current_positions")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Parse JSON + EXPLODE ARRAY
df_parsed = (
    df_kafka
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select(explode(col("data")).alias("pos"))
    .select("pos.*")
)

# 5. Transformation
df_final = (
    df_parsed
    .withColumn("end_date", to_date(col("endDate")))
    .select(
        col("title").alias("market_title"),
        col("asset").alias("asset_id"),
        col("size").alias("size"),

        when(
            col("end_date") > current_date(),
            lit("open")
        ).otherwise(
            lit("closed")
        ).alias("status"),

        col("realizedPnl").alias("realized_pnl"),
        col("proxyWallet").alias("wallet_address"),
        col("end_date")
    )
)

# 6. Send alert
def send_message(wallet, title, size):

    url = "http://delfos-anomalies-notifier:8000/notify"

    if wallet is None or title is None:
        print("Skipping null row")
        return

    payload = {
        "alert_id": "0",
        "sub_type": "SUSPECT_TRADE",
        "payload": {
            "wallet": str(wallet),
            "title": str(title),
            "size": float(size or 0)
        },
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        response = requests.post(url, json=payload, timeout=20)
        print("Status:", response.status_code)
        print(response.text)

    except Exception as e:
        print(f"[NOTIFICATION ERROR] {e}")


# 7. Upsert to Postgres
def upsert_batch_to_postgres(batch_df):

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    rows = batch_df.collect()

    for row in rows:

        cursor.execute("""
            INSERT INTO trade_sospechosos (
                market_title,
                asset_id,
                status,
                realized_pnl,
                wallet_address,
                notified
            )
            VALUES (%s, %s, %s, %s, %s, FALSE)

            ON CONFLICT (wallet_address, asset_id)
            DO UPDATE SET
                market_title = EXCLUDED.market_title,
                status = EXCLUDED.status,
                realized_pnl = EXCLUDED.realized_pnl
        """, (
            row["market_title"],
            row["asset_id"],
            row["status"],
            row["size"],
            row["wallet_address"]
        ))

    conn.commit()
    conn.close()


# 8. Notifications
def process_notifications():

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, market_title, wallet_address, realized_pnl
        FROM trade_sospechosos
        WHERE notified = FALSE
    """)

    rows = cursor.fetchall()

    for row in rows:

        trade_id = row[0]

        send_message(
            wallet=row[2],
            title=row[1],
            size=row[3] if row[3] else 0
        )

        cursor.execute("""
            UPDATE trade_sospechosos
            SET notified = TRUE
            WHERE id = %s
        """, (trade_id,))

    conn.commit()
    conn.close()


# 9. Batch processing
def write_to_db(batch_df, batch_id):

    print(f"Batch {batch_id}")

    if batch_df.isEmpty():
        print("Empty batch")
        return

    batch_df.persist()

    df_db = batch_df.dropDuplicates(["wallet_address", "asset_id"])

    upsert_batch_to_postgres(df_db)

    process_notifications()

    batch_df.unpersist()


# 10. STREAM
query = (
    df_final.writeStream
    .foreachBatch(write_to_db)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/trade_sospechosos")
    .start()
)

query.awaitTermination()