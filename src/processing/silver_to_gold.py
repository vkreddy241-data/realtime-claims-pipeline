"""
silver_to_gold.py
Aggregates Silver claims into Gold-layer summaries for Synapse Analytics.
Gold tables: claims_daily_summary, member_monthly_utilization, provider_scorecard
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (approx_count_distinct, avg, col, count,
    countDistinct, current_timestamp, lit, percentile_approx, sum as _sum)
from src.utils.config import load_config
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def build_daily_summary(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df
        .groupBy("payer_id", "claim_type", "service_date_dt", "claim_month")
        .agg(
            count("claim_id").alias("claim_count"),
            countDistinct("member_id").alias("unique_members"),
            _sum("claim_amount").alias("total_claim_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
            percentile_approx("claim_amount", 0.5).alias("median_claim_amount"),
            percentile_approx("claim_amount", 0.95).alias("p95_claim_amount"),
            count(col("status") == "denied").alias("denied_count"),
        )
        .withColumn("denial_rate", col("denied_count") / col("claim_count"))
        .withColumn("_gold_loaded_at", current_timestamp())
        .withColumn("_gold_table", lit("claims_daily_summary"))
    )

def build_member_utilization(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df
        .groupBy("member_id", "payer_id", "claim_month")
        .agg(
            count("claim_id").alias("claim_count"),
            _sum("claim_amount").alias("total_spend"),
            countDistinct("claim_type").alias("distinct_service_types"),
            approx_count_distinct("provider_npi").alias("distinct_providers"),
            _sum((col("claim_type") == "PHARMACY").cast("int")).alias("pharmacy_claim_count"),
            _sum((col("claim_type") == "MEDICAL").cast("int")).alias("medical_claim_count"),
        )
        .withColumn("avg_spend_per_claim", col("total_spend") / col("claim_count"))
        .withColumn("_gold_loaded_at", current_timestamp())
        .withColumn("_gold_table", lit("member_monthly_utilization"))
    )

def build_provider_scorecard(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.filter(col("provider_npi").isNotNull())
        .groupBy("provider_npi", "payer_id", "claim_month")
        .agg(
            count("claim_id").alias("total_claims"),
            countDistinct("member_id").alias("unique_patients"),
            _sum("claim_amount").alias("total_billed"),
            avg("claim_amount").alias("avg_claim_amount"),
            percentile_approx("claim_amount", 0.5).alias("median_claim_amount"),
            count(col("status") == "denied").alias("denied_claims"),
            count(col("status") == "adjusted").alias("adjusted_claims"),
        )
        .withColumn("denial_rate",     col("denied_claims")   / col("total_claims"))
        .withColumn("adjustment_rate", col("adjusted_claims") / col("total_claims"))
        .withColumn("_gold_loaded_at", current_timestamp())
        .withColumn("_gold_table", lit("provider_scorecard"))
    )

def write_gold_partition(df: DataFrame, path: str, partition_cols: list, spark: SparkSession):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        df.write.format("delta").mode("overwrite")
        .partitionBy(*partition_cols)
        .save(path)
    )
    logger.info("Gold write complete — path: %s", path)

def run(env: str = "dev"):
    cfg   = load_config(env=env)
    spark = (
        SparkSession.builder.appName("claims-silver-to-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    silver_path = cfg["delta_lake"]["silver_path"]
    gold_base   = cfg["delta_lake"]["gold_path"]
    silver_df   = spark.read.format("delta").load(silver_path)

    write_gold_partition(build_daily_summary(silver_df),
        f"{gold_base}/claims_daily_summary", ["claim_month", "payer_id", "claim_type"], spark)
    write_gold_partition(build_member_utilization(silver_df),
        f"{gold_base}/member_monthly_utilization", ["claim_month", "payer_id"], spark)
    write_gold_partition(build_provider_scorecard(silver_df),
        f"{gold_base}/provider_scorecard", ["claim_month", "payer_id"], spark)

    logger.info("Silver to Gold complete. All three Gold tables refreshed.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    args = parser.parse_args()
    run(env=args.env)
