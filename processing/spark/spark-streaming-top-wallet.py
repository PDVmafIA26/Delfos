from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2

# =========================================================
# 1. Spark Session
# =========================================================

spark = SparkSession.builder \
    .appName("KafkaTopWalletUpsert") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# 2. Schema REAL del JSON Kafka
# =========================================================

holder_schema = StructType([
    StructField("proxyWallet", StringType()),
    StructField("bio", StringType()),
    StructField("asset", StringType()),
    StructField("pseudonym", StringType()),
    StructField("amount", DoubleType()),
    StructField("displayUsernamePublic", BooleanType()),
    StructField("outcomeIndex", IntegerType()),
    StructField("name", StringType()),
    StructField("profileImage", StringType()),
    StructField("profileImageOptimized", StringType())
])

token_schema = ArrayType(
    StructType([
        StructField("token", StringType()),
        StructField("holders", ArrayType(holder_schema))
    ])
)

# =========================================================
# 3. Kafka Source
# =========================================================

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "top_wallets") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df_raw.selectExpr(
    "CAST(value AS STRING) as json"
)

# =========================================================
# 4. Parse JSON
# =========================================================

parsed = json_df.select(
    from_json(col("json"), token_schema).alias("data")
)

# =========================================================
# 5. Explode arrays
# =========================================================

exploded = parsed.select(
    explode(col("data")).alias("token_data")
)

exploded_holders = exploded.select(
    explode(col("token_data.holders")).alias("holder")
)

# =========================================================
# 6. Select columns
# =========================================================

holders_df = exploded_holders.select(
    trim(col("holder.proxyWallet")).alias("wallet_address"),
    trim(col("holder.asset")).alias("asset_id"),
    col("holder.amount").cast(DoubleType()).alias("amount")
)

# =========================================================
# 7. JDBC Config
# =========================================================

jdbc_url = "jdbc:postgresql://postgres:5432/markets"

jdbc_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# =========================================================
# 8. UPSERT FUNCTION
# =========================================================

def upsert_to_postgres(batch_df, batch_id):

    print(f"Processing batch {batch_id}")

    if batch_df.rdd.isEmpty():
        print("Empty batch")
        return

    batch_df.persist()

    # =====================================================
    # LEER outcome_tokens EN CADA BATCH
    # ESTO SOLUCIONA EL PROBLEMA
    # =====================================================

    outcome_tokens_df = spark.read.jdbc(
        url=jdbc_url,
        table="outcome_tokens",
        properties=jdbc_props
    ).select(
        trim(col("asset_id")).alias("asset_id"),
        trim(col("condition_id")).alias("condition_id")
    )

    # =====================================================
    # JOIN
    # =====================================================

    enriched_df = batch_df.join(
        broadcast(outcome_tokens_df),
        on="asset_id",
        how="left"
    ).select(
        col("wallet_address"),
        col("condition_id"),
        col("asset_id"),
        col("amount")
    )

    print("Sample enriched rows:")

    enriched_df.show(20, False)

    # =====================================================
    # OPCIONAL:
    # Guardar huérfanos sin condition_id
    # =====================================================

    missing_df = enriched_df.filter(
        col("condition_id").isNull()
    )

    matched_df = enriched_df.filter(
        col("condition_id").isNotNull()
    )

    print("Matched rows:")
    print(matched_df.count())

    print("Missing rows:")
    print(missing_df.count())

    # =====================================================
    # WRITE STAGING
    # =====================================================

    matched_df.write.jdbc(
        url=jdbc_url,
        table="top_wallets_staging",
        mode="overwrite",
        properties=jdbc_props
    )

    # =====================================================
    # UPSERT
    # =====================================================

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO top_wallets (
            wallet_address,
            condition_id,
            asset_id,
            amount
        )
        SELECT
            wallet_address,
            condition_id,
            asset_id,
            amount
        FROM top_wallets_staging

        ON CONFLICT (wallet_address, condition_id)
        DO UPDATE SET
            amount = EXCLUDED.amount,
            asset_id = EXCLUDED.asset_id
    """)

    conn.commit()

    cursor.close()
    conn.close()

    batch_df.unpersist()

    print(f"Batch {batch_id} completed")

# =========================================================
# 9. STREAM START
# =========================================================

query = holders_df.writeStream \
    .foreachBatch(upsert_to_postgres) \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/top_wallets_upsert"
    ) \
    .start()

print("Streaming started...")

query.awaitTermination()