from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2

# =========================================================
# 1. Spark Session
# =========================================================

spark = SparkSession.builder \
    .appName("KafkaUserUpsert") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# 2. RAW KAFKA SCHEMA
# =========================================================

raw_position_schema = StructType([
    StructField("proxyWallet", StringType()),
    StructField("title", StringType()),
    StructField("outcome", StringType()),
    StructField("realizedPnl", DoubleType()),
    StructField("timestamp", LongType())
])

raw_schema = ArrayType(raw_position_schema)

# =========================================================
# 3. YOUR INTERNAL SCHEMAS
# =========================================================

position_schema = StructType([
    StructField("market_title", StringType()),
    StructField("outcome", StringType()),
    StructField("realized_pnl", DoubleType()),
    StructField("status", StringType())
])

wallet_schema = StructType([
    StructField("wallet_address", StringType()),
    StructField("profile", StructType([
        StructField("name", StringType())
    ])),
    StructField("trading", StructType([
        StructField("positions", ArrayType(position_schema))
    ]))
])

schema = ArrayType(wallet_schema)

# =========================================================
# 4. READ KAFKA
# =========================================================

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "user_info") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json")

# =========================================================
# 5. PARSE REAL JSON
# =========================================================

parsed = json_df.select(
    explode(
        from_json(col("json"), raw_schema)
    ).alias("raw_pos")
).filter(
    col("raw_pos.proxyWallet").isNotNull()
)

# =========================================================
# 6. TRANSFORM TO YOUR STRUCTURE
# =========================================================

positions = parsed.select(

    col("raw_pos.proxyWallet").alias("wallet_address"),

    struct(
        col("raw_pos.title").alias("market_title"),

        col("raw_pos.outcome").alias("outcome"),

        col("raw_pos.realizedPnl").alias("realized_pnl"),

        when(
            col("raw_pos.realizedPnl") > 0,
            "WIN"
        ).otherwise("LOSE").alias("status")

    ).alias("pos")
)

# =========================================================
# 7. METRICS
# =========================================================

metrics = positions.select(
    col("wallet_address"),
    col("pos.realized_pnl").alias("pnl")
)

agg = metrics.groupBy("wallet_address").agg(

    sum(
        when(col("pnl") > 0, col("pnl"))
        .otherwise(0)
    ).alias("total_won"),

    sum(
        when(col("pnl") < 0, col("pnl"))
        .otherwise(0)
    ).alias("total_lost"),

    sum(col("pnl")).alias("net_pnl"),

    count("*").alias("total_positions"),

    sum(
        when(col("pnl") > 0, 1)
        .otherwise(0)
    ).alias("winning_positions")
)

result = agg.withColumn(
    "win_rate",

    when(
        col("total_positions") > 0,

        col("winning_positions") /
        col("total_positions")

    ).otherwise(0)
).select(
    col("wallet_address"),
    col("total_won"),
    col("total_lost"),
    col("net_pnl"),
    col("total_positions").alias("total_position"),
    col("win_rate")
)

# =========================================================
# DEBUG
# =========================================================

debug = result.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", False) \
    .start()

# =========================================================
# 8. UPSERT POSTGRES
# =========================================================

def upsert_to_postgres(batch_df, batch_id):

    print(f"Batch {batch_id}")

    rows = batch_df.count()

    print(f"Rows: {rows}")

    if rows == 0:
        return

    batch_df.persist()

    url = "jdbc:postgresql://postgres:5432/markets"

    props = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    batch_df.write.jdbc(
        url=url,
        table="usuarios_staging",
        mode="overwrite",
        properties=props
    )

    conn = psycopg2.connect(
        host="postgres",
        database="markets",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    cursor.execute("""

        INSERT INTO usuarios AS t (

            wallet_address,
            total_won,
            total_lost,
            net_pnl,
            total_position,
            es_sospechoso

        )

        SELECT
            wallet_address,
            total_won,
            total_lost,
            net_pnl,
            total_position,
            FALSE

        FROM usuarios_staging

        ON CONFLICT (wallet_address)

        DO UPDATE SET

            total_won = t.total_won + EXCLUDED.total_won,

            total_lost = t.total_lost + EXCLUDED.total_lost,

            net_pnl = t.net_pnl + EXCLUDED.net_pnl,

            total_position = t.total_position + EXCLUDED.total_position,

            es_sospechoso = (

                (
                    (t.total_won + EXCLUDED.total_won)

                    /

                    NULLIF(
                        (t.total_position + EXCLUDED.total_position),
                        0
                    )

                ) >= 0.9

                AND
                (t.total_position + EXCLUDED.total_position) >= 1

                AND
                (t.total_won + EXCLUDED.total_won) >= 10000
            )

    """)

    conn.commit()

    cursor.close()
    conn.close()

    batch_df.unpersist()

# =========================================================
# 9. START STREAM
# =========================================================

query = result.writeStream \
    .foreachBatch(upsert_to_postgres) \
    .outputMode("update") \
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/users_upsert"
    ) \
    .start()

query.awaitTermination()