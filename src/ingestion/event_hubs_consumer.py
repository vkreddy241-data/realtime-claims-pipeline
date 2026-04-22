"""
event_hubs_consumer.py
Reads claim events from Azure Event Hubs and writes raw records to Bronze Delta Lake on ADLS Gen2.
"""
import logging
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType, TimestampType)
from src.utils.config import load_config
from src.utils.logging_utils import get_logger
from src.utils.retry import with_retry

logger = get_logger(__name__)

CLAIM_EVENT_SCHEMA = StructType([
    StructField("claim_id",         StringType(),    nullable=False),
    StructField("member_id",        StringType(),    nullable=False),
    StructField("payer_id",         StringType(),    nullable=False),
    StructField("provider_npi",     StringType(),    nullable=True),
    StructField("claim_type",       StringType(),    nullable=False),
    StructField("claim_amount",     DoubleType(),    nullable=False),
    StructField("service_date",     TimestampType(), nullable=False),
    StructField("received_at",      TimestampType(), nullable=False),
    StructField("diagnosis_codes",  StringType(),    nullable=True),
    StructField("procedure_codes",  StringType(),    nullable=True),
    StructField("status",           StringType(),    nullable=False),
])

def build_spark_session(app_name: str = "claims-bronze-writer") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_from_event_hubs(spark: SparkSession, cfg: dict):
    eh_conf = {
        "eventhubs.connectionString": spark.conf.get("spark.secret.eventhubs.connectionString"),
        "eventhubs.consumerGroup":    cfg["event_hubs"]["consumer_group"],
        "eventhubs.startingPosition": '{"offset": "@latest", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}',
        "eventhubs.maxEventsPerTrigger": cfg["event_hubs"].get("max_events_per_trigger", 50000),
    }
    raw_stream = spark.readStream.format("eventhubs").options(**eh_conf).load()
    logger.info("Event Hubs stream reader initialized — consumer group: %s", cfg["event_hubs"]["consumer_group"])
    return raw_stream

def parse_and_validate(raw_stream, schema: StructType):
    parsed = raw_stream.select(
        from_json(col("body").cast("string"), schema).alias("data"),
        col("body").cast("string").alias("raw_body"),
        col("enqueuedTime").alias("enqueued_at"),
    )
    good_df = (
        parsed.filter(col("data.claim_id").isNotNull() & col("data.member_id").isNotNull())
        .select(col("data.*"), col("enqueued_at"), current_timestamp().alias("_ingested_at"), lit("event_hubs").alias("_source"))
    )
    bad_df = (
        parsed.filter(col("data.claim_id").isNull() | col("data.member_id").isNull())
        .select(col("raw_body"), col("enqueued_at"), current_timestamp().alias("_ingested_at"), lit("schema_validation_failure").alias("_failure_reason"))
    )
    return good_df, bad_df

def write_bronze(good_df, bad_df, cfg: dict):
    bronze_path     = cfg["delta_lake"]["bronze_path"]
    dlq_path        = cfg["delta_lake"].get("dlq_path", bronze_path.rstrip("/") + "_dlq")
    checkpoint_base = cfg["delta_lake"]["checkpoint_store"]
    good_query = (
        good_df.writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/bronze_good")
        .option("mergeSchema", "true")
        .partitionBy("claim_type", "service_date")
        .trigger(processingTime=cfg.get("trigger_interval", "60 seconds"))
        .start(bronze_path)
    )
    bad_query = (
        bad_df.writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/bronze_dlq")
        .trigger(processingTime="5 minutes")
        .start(dlq_path)
    )
    logger.info("Bronze writer started — path: %s, DLQ: %s", bronze_path, dlq_path)
    return good_query, bad_query

@with_retry(max_attempts=3, backoff_seconds=10)
def run(env: str = "dev", config_path: Optional[str] = None):
    cfg   = load_config(env=env, path=config_path)
    spark = build_spark_session()
    raw_stream        = read_from_event_hubs(spark, cfg)
    good_df, bad_df   = parse_and_validate(raw_stream, CLAIM_EVENT_SCHEMA)
    good_query, bad_query = write_bronze(good_df, bad_df, cfg)
    try:
        good_query.awaitTermination()
        bad_query.awaitTermination()
    except Exception as exc:
        logger.error("Streaming query terminated with error: %s", exc)
        good_query.stop()
        bad_query.stop()
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    run(env=args.env, config_path=args.config)
