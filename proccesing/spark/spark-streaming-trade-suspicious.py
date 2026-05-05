from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, lit, when, to_date, current_date
from datetime import datetime
import requests

# 1. Spark Session
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

# 3. read from Kafka
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

# 5. Transformation to the SQL model
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
        ).otherwise(lit("closed")).alias("status"),
        col("realizedPnl").alias("realized_pnl"),
        col("proxyWallet").alias("wallet_address"),
        col("end_date")
    )
)

def send_message(row):
    

    url = f"https://api.telegram.org/bot/notify"

    try:
        requests.post(url, timeout=20)
    except Exception as e:
        print(f"Error sending Telegram: {e}")

# 6. Writing to PostgreSQL (JDBC)
def write_to_db(batch_df, batch_id):
    batch_df.persist()
    df_db = batch_df.select(
        "market_title",
        "asset_id",
        "status",
        "realized_pnl",
        "wallet_address"
    )
    (
        df_db.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/markets")
        .option("dbtable", "trade_sospechosos")
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    def send_partition(partition):
        today = datetime.now().date()
        for row in partition:
            if row.end_date is None:
                continue

        # convertir string a date si viene como string
            try:
                end_date = row.end_date
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

                if end_date > today:
                    send_message(row)

            except Exception as e:
                print(f"Error parsing date: {e}")

    batch_df.foreachPartition(send_partition)

    batch_df.unpersist()

query = (
    df_final.writeStream
    .foreachBatch(write_to_db)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/trade_sospechosos")
    .start()
)

query.awaitTermination()