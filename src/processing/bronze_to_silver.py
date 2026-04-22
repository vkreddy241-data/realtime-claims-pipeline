"""
bronze_to_silver.py
Reads Bronze Delta table and applies cleansing, deduplication, and validation to produce Silver layer.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (array_distinct, col, current_timestamp, date_trunc,
    row_number, split, to_date, trim, upper, when)
from pyspark.sql.window import Window
from src.utils.config import load_config
from src.utils.logging_utils import get_logger
from delta.tables import DeltaTable

logger = get_logger(__name__)

def standardize_claim_type(df: DataFrame) -> DataFrame:
    valid = ["MEDICAL", "PHARMACY", "DENTAL", "VISION", "BEHAVIORAL"]
    return df.withColumn(
        "claim_type",
        when(upper(trim(col("claim_type"))).isin(valid), upper(trim(col("claim_type")))).otherwise("OTHER"),
    )

def parse_code_arrays(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumnRenamed("diagnosis_codes", "_raw_diagnosis_codes")
        .withColumnRenamed("procedure_codes", "_raw_procedure_codes")
        .withColumn("diagnosis_codes",
            when(col("_raw_diagnosis_codes").isNotNull(),
                 array_distinct(split(trim(col("_raw_diagnosis_codes")), r"\|"))).otherwise(None))
        .withColumn("procedure_codes",
            when(col("_raw_procedure_codes").isNotNull(),
                 array_distinct(split(trim(col("_raw_procedure_codes")), r"\|"))).otherwise(None))
    )

def add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("service_date_dt", to_date(col("service_date")))
        .withColumn("claim_month",     date_trunc("month", col("service_date")))
        .withColumn("_silver_loaded_at", current_timestamp())
    )

def deduplicate(df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import desc
    window = Window.partitionBy("claim_id").orderBy(desc("received_at"))
    return df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")

REQUIRED_FIELDS = ["claim_id", "member_id", "payer_id", "claim_amount", "service_date", "status"]

def split_valid_quarantine(df: DataFrame) -> tuple:
    from pyspark.sql.functions import array, array_remove, lit
    checks = []
    for field in REQUIRED_FIELDS:
        checks.append(when(col(field).isNull(), lit(f"null:{field}")).otherwise(lit(None)))
    checks.append(when(col("claim_amount") <= 0, lit("invalid:claim_amount_not_positive")).otherwise(lit(None)))
    checks.append(when(col("claim_amount") > 10_000_000, lit("suspect:claim_amount_exceeds_10M")).otherwise(lit(None)))
    df = df.withColumn("_failure_reasons", array_remove(array(*checks), None))
    valid_df     = df.filter(col("_failure_reasons").isNull() | (col("_failure_reasons").getItem(0).isNull()))
    quarantine_df = df.filter(col("_failure_reasons").isNotNull() & (col("_failure_reasons").getItem(0).isNotNull()))
    return valid_df.drop("_failure_reasons"), quarantine_df

def upsert_to_silver(valid_df: DataFrame, silver_path: str, spark: SparkSession):
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("existing")
            .merge(valid_df.alias("incoming"), "existing.claim_id = incoming.claim_id")
            .whenMatchedUpdate(condition="incoming.received_at > existing.received_at",
                               set={c: f"incoming.{c}" for c in valid_df.columns})
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("MERGE INTO Silver complete — path: %s", silver_path)
    else:
        valid_df.write.format("delta").mode("overwrite").partitionBy("claim_month", "claim_type").save(silver_path)
        logger.info("Silver table created (first run) — path: %s", silver_path)

def run(env: str = "dev"):
    cfg   = load_config(env=env)
    spark = (
        SparkSession.builder.appName("claims-bronze-to-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    bronze_path     = cfg["delta_lake"]["bronze_path"]
    silver_path     = cfg["delta_lake"]["silver_path"]
    quarantine_path = cfg["delta_lake"].get("quarantine_path", silver_path.rstrip("/") + "_quarantine")
    bronze_df = spark.read.format("delta").load(bronze_path)
    transformed = (bronze_df.transform(standardize_claim_type).transform(parse_code_arrays)
                             .transform(deduplicate).transform(add_derived_columns))
    valid_df, quarantine_df = split_valid_quarantine(transformed)
    upsert_to_silver(valid_df, silver_path, spark)
    if quarantine_df.count() > 0:
        quarantine_df.write.format("delta").mode("append").save(quarantine_path)
        logger.warning("Quarantined %d records", quarantine_df.count())
    logger.info("Bronze to Silver complete.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    args = parser.parse_args()
    run(env=args.env)
