from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2

# 1. Spark Session
spark = SparkSession.builder \
    .appName("WhaleDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema JSON — estructura real del canal user de Polymarket WebSocket
# Los datos llegan tal cual desde la API, sin transformación en ingesta.
maker_order_schema = StructType([
    StructField("asset_id",       StringType()),   
    StructField("matched_amount", StringType()),    
    StructField("order_id",       StringType()),
    StructField("outcome",        StringType()),    
    StructField("owner",          StringType()),    
    StructField("price",          StringType()),   
])

schema = StructType([
    StructField("asset_id",       StringType()),   
    StructField("event_type",     StringType()),   
    StructField("id",             StringType()),   
    StructField("last_update",    StringType()),   
    StructField("maker_orders",   ArrayType(maker_order_schema)),
    StructField("market",         StringType()),    
    StructField("matchtime",      StringType()),    
    StructField("outcome",        StringType()),    
    StructField("owner",          StringType()),   
    StructField("price",          StringType()),    
    StructField("side",           StringType()),    
    StructField("size",           StringType()),    
    StructField("status",         StringType()),    
    StructField("taker_order_id", StringType()),
    StructField("timestamp",      StringType()),    
    StructField("trade_owner",    StringType()),    
    StructField("type",           StringType()),    
])

# 3. Read from Kafka
# !!!!!
# Tópico a confirmar con ingesta — nombre provisional
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "top_wallets") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_ts")

# 4. Parse JSON y filtrar solo eventos de tipo TRADE
parsed = json_df.select(
    from_json(col("json"), schema).alias("data"),
    col("kafka_ts")
)

trades = parsed \
    .filter(col("data.type") == "TRADE") \
    .filter(col("data.status") == "MATCHED") \
    .select(
        # !!!!!
        # Para identificar ballenas necesitamos la wallet, "owner" en la API de Polymarket es un UUID interno
        #   - Opción A: usar trade_owner si el productor de ingesta la resuelve a wallet.
        #   - Opción B: hacer join posterior con una tabla owner_uuid -> wallet_address.
        col("data.trade_owner").alias("user_id"), # !!! Por ahora usamos trade_owner como identificador
        col("data.market").alias("condition_id"),    
        col("data.asset_id"),                         
        col("data.size").cast(DoubleType()).alias("amount"),    
        # El volumen en USD = size * price (ambos vienen como string)
        (col("data.size").cast(DoubleType()) *
         col("data.price").cast(DoubleType())).alias("amount_usd"),
        col("data.outcome"),                         
        col("data.side"),                             
        col("kafka_ts").alias("event_time")
    )

# 5. Ventana deslizante: 1 hora evaluada cada 5 minutos
windowed = trades \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "1 hour", "5 minutes"),
        col("user_id"),
        col("condition_id")
    ).agg(
        sum("amount_usd").alias("market_exposure"),
        count("*").alias("num_trades"),
        last("outcome").alias("position_type"),
        last("side").alias("side")
    )

# Agregar exposición total del usuario en todos sus mercados de la ventana
total_volume = windowed.groupBy(
    col("window"),
    col("user_id")
).agg(
    sum("market_exposure").alias("total_volume_window"),
    sum("num_trades").alias("total_trades"),
    collect_list(
        struct(col("condition_id"), col("market_exposure"), col("position_type"))
    ).alias("market_positions")
)

# 6. Enriquecimiento con Capa Oro y detección de ballenas
gold_layer_url = "jdbc:postgresql://localhost:5432/markets"
gold_layer_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def enrich_and_detect_whales(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    batch_df.persist()

    # Join con tabla USUARIOS (Capa Oro) para obtener histórico
    # !!!!
    # la tabla USUARIOS usa wallet_address como PK.
    # Si user_id aquí es un UUID interno de Polymarket, se necesita una tabla de mapeo uuid wallet_address. 
    # Pendiente de confirmar con ingesta.
    usuarios_df = spark.read.jdbc(
        url=gold_layer_url,
        table="usuarios",
        properties=gold_layer_props
    ).select(
        col("wallet_address").alias("user_id"),
        col("net_pnl"),
        col("total_position"),
        col("es_sospechoso"),
        when(col("total_position") > 0,
             col("total_won") / col("total_position")
        ).otherwise(0).alias("avg_bet_size"),
        when(col("net_pnl") >= 10000, "HIGH")
        .when(col("net_pnl") >= 1000,  "MEDIUM")
        .otherwise("LOW").alias("historical_pnl")
    )

    enriched = batch_df.join(usuarios_df, on="user_id", how="left")

    # Join con MERCADOS_MASTER para calcular impacto relativo
    mercados_df = spark.read.jdbc(
        url=gold_layer_url,
        table="mercados_master",
        properties=gold_layer_props
    ).select(
        col("conditionId").alias("condition_id"),
        col("liquidity").alias("market_liquidity")
    )

    # Explotar market_positions para calcular impacto por mercado
    exploded = enriched.select(
        col("window"), col("user_id"), col("total_volume_window"),
        col("total_trades"), col("historical_pnl"), col("avg_bet_size"),
        col("es_sospechoso"),
        explode(col("market_positions")).alias("mp")
    ).select(
        col("window"), col("user_id"), col("total_volume_window"),
        col("total_trades"), col("historical_pnl"), col("avg_bet_size"),
        col("es_sospechoso"),
        col("mp.condition_id"), col("mp.market_exposure"), col("mp.position_type")
    )

    with_liquidity = exploded.join(mercados_df, on="condition_id", how="left")

    with_impact = with_liquidity.withColumn(
        "market_impact_pct",
        when(col("market_liquidity") > 0,
             (col("market_exposure") / col("market_liquidity")) * 100
        ).otherwise(0)
    )

    max_impact = with_impact.groupBy(
        "user_id", "total_volume_window", "total_trades",
        "historical_pnl", "avg_bet_size", "es_sospechoso", "window"
    ).agg(
        max("market_impact_pct").alias("market_impact_pct"),
        first("position_type").alias("position_type"),
        first("condition_id").alias("top_condition_id")
    )

    # Reglas de detección 
    whales = max_impact.filter(
        (col("total_volume_window") > 50000) |
        (col("market_impact_pct") > 2) |
        (
            (col("historical_pnl") == "HIGH") &
            (col("avg_bet_size") > 5000)
        )
    )

    if whales.count() == 0:
        batch_df.unpersist()
        return

    whales_to_save = whales.select(
        col("user_id"),
        col("total_volume_window"),
        col("market_impact_pct"),
        col("historical_pnl"),
        col("avg_bet_size"),
        col("position_type"),
        col("top_condition_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end")
    )

    whales_to_save.write.jdbc(
        url=gold_layer_url,
        table="whales_staging",
        mode="overwrite",
        properties=gold_layer_props
    )

    conn = psycopg2.connect(
        dbname="markets", user="postgres",
        password="postgres", host="localhost", port="5432"
    )
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO anomalias (category, sub_tpye, payload, created_at)
        SELECT
            'WHALE_DETECTION',
            CASE
                WHEN total_volume_window > 50000 AND market_impact_pct > 2 THEN 'HIGH_VOLUME_HIGH_IMPACT'
                WHEN total_volume_window > 50000 THEN 'HIGH_VOLUME'
                WHEN market_impact_pct > 2      THEN 'HIGH_IMPACT'
                ELSE 'SMART_MONEY'
            END,
            json_build_object(
                'user_id',           user_id,
                'market_impact_pct', market_impact_pct,
                'historical_pnl',    historical_pnl,
                'position_type',     position_type,
                'total_volume_usd',  total_volume_window,
                'avg_bet_size',      avg_bet_size,
                'window_start',      window_start,
                'window_end',        window_end
            )::text,
            NOW()
        FROM whales_staging
    """)

    cursor.execute("""
        UPDATE usuarios
        SET es_sospechoso = TRUE
        WHERE wallet_address IN (SELECT user_id FROM whales_staging)
          AND es_sospechoso = FALSE
    """)

    conn.commit()
    cursor.close()
    conn.close()

    batch_df.unpersist()

# 7. Write Stream
query = total_volume.writeStream \
    .foreachBatch(enrich_and_detect_whales) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/whale_detection") \
    .start()

query.awaitTermination()