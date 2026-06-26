# ============================================
# Spark Structured Streaming — Fraud Engine
# fraud-detection-pipeline/streaming/spark_stream.py
# ============================================

import sys
import os
sys.stdout.reconfigure(encoding='utf-8')

# ── Path Setup (handles spaces in Windows paths) ──────────────────────
STREAM_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR   = os.path.abspath(os.path.join(STREAM_DIR, '..'))
sys.path.append(BASE_DIR)

# Checkpoint paths — forward slashes for Spark on Windows
CHECKPOINT_TXN = os.path.join(
    BASE_DIR, 'data_output', 'checkpoints', 'transactions'
).replace('\\', '/')

CHECKPOINT_MERCHANT = os.path.join(
    BASE_DIR, 'data_output', 'checkpoints', 'merchants'
).replace('\\', '/')

CHECKPOINT_METRICS = os.path.join(
    BASE_DIR, 'data_output', 'checkpoints', 'metrics'
).replace('\\', '/')

print(f"BASE_DIR         : {BASE_DIR}")
print(f"CHECKPOINT_TXN   : {CHECKPOINT_TXN}")
print(f"CHECKPOINT_MERCHANT: {CHECKPOINT_MERCHANT}")
print(f"CHECKPOINT_METRICS : {CHECKPOINT_METRICS}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit,
    round as spark_round,
    current_timestamp, unix_timestamp, window,
    count, avg,
    sum as spark_sum,
    udf
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    IntegerType, TimestampType
)
import configs.config as cfg
import psycopg2
from datetime import datetime

# ── Schema ────────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  True),
    StructField("amount",            DoubleType(),  True),
    StructField("merchant",          StringType(),  True),
    StructField("merchant_category", StringType(),  True),
    StructField("card_number",       StringType(),  True),
    StructField("country",           StringType(),  True),
    StructField("home_country",      StringType(),  True),
    StructField("timestamp",         StringType(),  True),
    StructField("produced_at",       DoubleType(),  True),
    StructField("v1",                DoubleType(),  True),
    StructField("v2",                DoubleType(),  True),
    StructField("v3",                DoubleType(),  True),
    StructField("actual_class",      IntegerType(), True),
    StructField("anomaly_score",     DoubleType(),  True),
    StructField("producer_label",    StringType(),  True),
    StructField("sequence_index",    IntegerType(), True),
])

# ── Risk Lists ────────────────────────────────────────────────────────
HIGH_RISK_COUNTRIES = ["Nigeria", "Russia", "Unknown", "Belarus"]
MEDIUM_RISK         = ["China", "Brazil", "Mexico", "Turkey"]
HIGH_RISK_CATS      = ["Crypto", "Wire Transfer",
                        "Gift Cards", "Jewelry"]
MEDIUM_CATS         = ["Travel", "Electronics"]


# ── Fraud Scoring UDF ─────────────────────────────────────────────────
def compute_fraud_score(amount, country, category,
                         anomaly_score, home_country):
    score = 0.0
    if amount is not None:
        if amount > 8000:   score += 0.40
        elif amount > 5000: score += 0.25
        elif amount > 2000: score += 0.10
        elif amount > 1000: score += 0.05
    if country is not None:
        if country in HIGH_RISK_COUNTRIES: score += 0.30
        elif country in MEDIUM_RISK:       score += 0.10
    if category is not None:
        if category in HIGH_RISK_CATS: score += 0.20
        elif category in MEDIUM_CATS:  score += 0.05
    if anomaly_score is not None:
        score += anomaly_score * 0.25
    if (home_country and country
            and home_country != country
            and country in HIGH_RISK_COUNTRIES):
        score += 0.15
    return round(min(score, 1.0), 3)


fraud_score_udf = udf(compute_fraud_score, DoubleType())


def classify_label(score):
    if score is None:   return "LEGIT"
    if score >= 0.65:   return "FRAUD"
    elif score >= 0.35: return "SUSPICIOUS"
    return "LEGIT"


classify_udf = udf(classify_label, StringType())


# ── Writers ───────────────────────────────────────────────────────────
def write_transactions_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", cfg.POSTGRES_URL) \
            .option("dbtable", "fraud_transactions") \
            .option("user", cfg.POSTGRES_USER) \
            .option("password", cfg.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        total      = batch_df.count()
        fraud      = batch_df.filter(
                        col("fraud_label") == "FRAUD").count()
        suspicious = batch_df.filter(
                        col("fraud_label") == "SUSPICIOUS").count()
        legit      = total - fraud - suspicious
        print(f"\n  [BATCH {batch_id}] -> PostgreSQL | "
              f"Total={total} FRAUD={fraud} "
              f"SUSPICIOUS={suspicious} LEGIT={legit}")
    except Exception as e:
        print(f"  [ERROR] Batch {batch_id}: {str(e)}")
        raise


def write_merchant_stats_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", cfg.POSTGRES_URL) \
            .option("dbtable", "merchant_stats") \
            .option("user", cfg.POSTGRES_USER) \
            .option("password", cfg.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"  [BATCH {batch_id}] Merchant stats -> PostgreSQL "
              f"({batch_df.count()} rows)")
    except Exception as e:
        print(f"  [ERROR] Merchant stats: {str(e)}")
        raise


def write_pipeline_metrics(batch_df, batch_id):
    """foreachBatch writer: inserts per-microbatch throughput and
    average latency into the pipeline_metrics table."""
    try:
        batch_count = batch_df.count()
        if batch_count == 0:
            return

        avg_latency_row = batch_df.agg(
            {"processing_latency_ms": "avg"}
        ).collect()[0][0]

        conn = psycopg2.connect(
            host=cfg.POSTGRES_HOST,
            port=cfg.POSTGRES_PORT,
            dbname=cfg.POSTGRES_DB,
            user=cfg.POSTGRES_USER,
            password=cfg.POSTGRES_PASSWORD
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_metrics
                        (recorded_at, batch_size, avg_latency_ms)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        datetime.utcnow(),
                        batch_count,
                        float(avg_latency_row) if avg_latency_row is not None
                        else None
                    )
                )
            conn.commit()
        finally:
            conn.close()

        print(f"  [METRICS {batch_id}] batch_size={batch_count} "
              f"avg_latency_ms="
              f"{avg_latency_row:.2f}" if avg_latency_row else "None")
    except Exception as e:
        print(f"  [ERROR] Metrics batch {batch_id}: {str(e)}")


# ── Main ──────────────────────────────────────────────────────────────
def main():
    print("=" * 62)
    print("  FRAUD DETECTION - SPARK STREAMING ENGINE")
    print("=" * 62)

    # ── Verify checkpoint dirs exist ──────────────────────
    for path in [CHECKPOINT_TXN, CHECKPOINT_MERCHANT, CHECKPOINT_METRICS]:
        os.makedirs(path, exist_ok=True)
        print(f"[OK] Checkpoint dir: {path}")

    # ── Spark Session ─────────────────────────────────────
    spark = SparkSession.builder \
        .appName(cfg.SPARK_APP_NAME) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config(
            "spark.streaming.stopGracefullyOnShutdown",
            "true"
        ) \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
            "org.postgresql:postgresql:42.6.0"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"[OK] Spark Session started")
    print(f"[OK] Kafka: {cfg.KAFKA_BROKER} | "
          f"Topic: {cfg.KAFKA_TOPIC}")

    # ── Read from Kafka ───────────────────────────────────
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", cfg.KAFKA_BROKER) \
        .option("subscribe", cfg.KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("[OK] Kafka stream connected")

    # ── Parse + Score ─────────────────────────────────────
    parsed = raw_stream.select(
        from_json(
            col("value").cast("string"),
            TRANSACTION_SCHEMA
        ).alias("data")
    ).select("data.*")

    scored = parsed \
        .withColumn(
            "fraud_score",
            fraud_score_udf(
                col("amount"),
                col("country"),
                col("merchant_category"),
                col("anomaly_score"),
                col("home_country")
            )
        ) \
        .withColumn(
            "fraud_label",
            classify_udf(col("fraud_score"))
        ) \
        .withColumn(
            "is_fraud",
            col("fraud_label") == lit("FRAUD")
        ) \
        .withColumn(
            "processed_at",
            current_timestamp()
        ) \
        .withColumn(
            "processing_latency_ms",
            (col("processed_at").cast("double") - col("produced_at")) * 1000
        )

    # ── Transaction output columns ────────────────────────
    transactions_out = scored.select(
        col("transaction_id"),
        col("amount"),
        col("merchant"),
        col("merchant_category"),
        col("card_number"),
        col("country"),
        col("fraud_score"),
        col("fraud_label"),
        col("is_fraud"),
        col("anomaly_score"),
        col("producer_label"),
        col("actual_class"),
        col("timestamp").cast(TimestampType()),
        col("processed_at")
    )

    # ── Stream 1: Transactions ────────────────────────────
    txn_query = transactions_out.writeStream \
        .foreachBatch(write_transactions_to_postgres) \
        .option("checkpointLocation", CHECKPOINT_TXN) \
        .trigger(processingTime="2 seconds") \
        .start()

    print("[OK] Transaction stream started (2s batches)")

    # ── Merchant Aggregation ──────────────────────────────
    merchant_agg = scored \
        .withColumn(
            "event_time",
            col("timestamp").cast(TimestampType())
        ) \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("merchant"),
            col("merchant_category")
        ) \
        .agg(
            count("*").alias("total_transactions"),
            spark_sum(
                when(col("fraud_label") == "FRAUD", 1)
                .otherwise(0)
            ).alias("fraud_count"),
            spark_sum(
                when(col("fraud_label") == "SUSPICIOUS", 1)
                .otherwise(0)
            ).alias("suspicious_count"),
            spark_round(avg("amount"), 2).alias("avg_amount"),
            spark_round(
                avg("fraud_score"), 3
            ).alias("avg_fraud_score")
        ) \
        .withColumn(
            "fraud_rate",
            spark_round(
                col("fraud_count") /
                col("total_transactions"), 4
            )
        ) \
        .select(
            col("merchant"),
            col("merchant_category"),
            col("total_transactions"),
            col("fraud_count"),
            col("suspicious_count"),
            col("fraud_rate"),
            col("avg_amount"),
            col("avg_fraud_score"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            current_timestamp().alias("updated_at")
        )

    # ── Stream 2: Merchant Stats ──────────────────────────
    merchant_query = merchant_agg.writeStream \
        .foreachBatch(write_merchant_stats_to_postgres) \
        .option("checkpointLocation", CHECKPOINT_MERCHANT) \
        .trigger(processingTime="2 seconds") \
        .outputMode("update") \
        .start()

    print("[OK] Merchant stats stream started (2s batches)")

    # ── Stream 3: Pipeline Metrics ────────────────────────
    metrics_query = scored.writeStream \
        .foreachBatch(write_pipeline_metrics) \
        .option("checkpointLocation", CHECKPOINT_METRICS) \
        .trigger(processingTime="2 seconds") \
        .start()

    print("[OK] Pipeline metrics stream started (2s batches)")
    print("\n" + "=" * 62)
    print("  STREAMING IN PROGRESS - Waiting for data...")
    print("  Press Ctrl+C to stop")
    print("=" * 62 + "\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()