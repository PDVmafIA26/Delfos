from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, lit

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
    StructField("title", StringType(), True),
    StructField("realizedPnl", DoubleType(), True)
])

# 3. read from Kafka
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "wallet_current_positions")
    .option("startingOffsets", "latest")
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
    .select(
        col("title").alias("market_title"),
        col("asset").alias("asset_id"),
        lit("open").alias("status"),
        col("realizedPnl").alias("realized_pnl"),
        col("proxyWallet").alias("wallet_address")
    )
)

# 6. Writing to PostgreSQL (JDBC)
def write_to_db(batch_df, batch_id):
    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/markets")
        .option("dbtable", "trade_sospechosos")
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

query = (
    df_final.writeStream
    .foreachBatch(write_to_db)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/trade_sospechosos")
    .start()
)

query.awaitTermination()