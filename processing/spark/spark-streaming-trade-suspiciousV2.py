from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType)
from pyspark.sql.functions import (col, from_json, lit, when, to_date, current_date)
from datetime import datetime
import requests
import psycopg2

# 1. Spark session

spark = (
    SparkSession.builder
    .appName("Kafka_to_Trade_Sospechosos")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Schema

schema = StructType([
    StructField("proxyWallet", StringType(), True),
    StructField("asset", StringType(), True),
    StructField("size", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("realizedPnl", DoubleType(), True),
    StructField("endDate", StringType(), True)
])

# 3. Read from kafka

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "wallet_current_positions")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Parse JSON

df_parsed = (
    df_kafka
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

# 5. Transoformation

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

def send_message(row):

    url = "http://localhost:8000/notify"

    payload = {
        "alert_id": "0",
        "sub_type": "SUSPECT_TRADE",
        "payload": {
            "wallet": row["wallet_address"],
            "title": row["market_title"],
            "size": float(row["size"])
        },
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=20
        )

        print(f"Status code: {response.status_code}")

    except Exception as e:
        print(f"[NOTIFICATION ERROR] {e}")

# 7. Upsert function

def upsert_batch_to_postgres(batch_df):

    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname="markets",
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
            row["realized_pnl"],
            row["wallet_address"]
        ))

    conn.commit()
    conn.close()

# 8. Send notifications only for new trades

def process_notifications():

    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id_autoincremental,
            market_title,
            wallet_address,
            size
        FROM trade_sospechosos
        WHERE notified = FALSE
    """)

    rows = cursor.fetchall()

    for row in rows:

        trade_id = row[0]

        payload = {
            "market_title": row[1],
            "wallet_address": row[2],
            "size": row[3] if row[3] is not None else 0
        }

        send_message(payload)

        cursor.execute("""
            UPDATE trade_sospechosos
            SET notified = TRUE
            WHERE id_autoincremental = %s
        """, (trade_id,))

    conn.commit()
    conn.close()

# 9. Proccess each batch

def write_to_db(batch_df, batch_id):

    print(f"Batch {batch_id} n")

    if batch_df.isEmpty():
        print("Empty batch")
        return

    # Persistimos porque lo usamos varias veces
    batch_df.persist()

    # Deduplicación

    df_db = (
        batch_df.dropDuplicates([
            "wallet_address",
            "asset_id"])
    )

    # Upsert

    upsert_batch_to_postgres(df_db)

    # Send notification

    process_notifications()

    batch_df.unpersist()

# 10. STREAM

query = (
    df_final.writeStream
    .foreachBatch(write_to_db)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/trade_sospechosos"
    )
    .start()
)

query.awaitTermination()