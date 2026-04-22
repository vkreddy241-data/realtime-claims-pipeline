"""
test_silver_transforms.py
Unit tests for Bronze -> Silver transformation functions.
Run with: pytest tests/ -v
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType, TimestampType)
from src.processing.bronze_to_silver import (
    standardize_claim_type, parse_code_arrays, deduplicate, split_valid_quarantine)

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder.master("local[2]").appName("claims-unit-tests")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate())

SCHEMA = StructType([
    StructField("claim_id",        StringType(),    True),
    StructField("member_id",       StringType(),    True),
    StructField("payer_id",        StringType(),    True),
    StructField("claim_type",      StringType(),    True),
    StructField("claim_amount",    DoubleType(),    True),
    StructField("service_date",    TimestampType(), True),
    StructField("received_at",     TimestampType(), True),
    StructField("status",          StringType(),    True),
    StructField("diagnosis_codes", StringType(),    True),
    StructField("procedure_codes", StringType(),    True),
])

def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)

class TestClaimTypeStandardization:
    def test_lowercase_normalized_to_upper(self, spark):
        df = make_df(spark, [("C001","M001","P001","medical",500.0,datetime(2024,1,15),datetime(2024,1,15),"submitted",None,None)])
        assert standardize_claim_type(df).collect()[0]["claim_type"] == "MEDICAL"

    def test_unknown_type_mapped_to_other(self, spark):
        df = make_df(spark, [("C002","M001","P001","radiology",200.0,datetime(2024,1,15),datetime(2024,1,15),"submitted",None,None)])
        assert standardize_claim_type(df).collect()[0]["claim_type"] == "OTHER"

    def test_whitespace_stripped(self, spark):
        df = make_df(spark, [("C003","M001","P001","  pharmacy  ",50.0,datetime(2024,1,15),datetime(2024,1,15),"submitted",None,None)])
        assert standardize_claim_type(df).collect()[0]["claim_type"] == "PHARMACY"

class TestCodeParsing:
    def test_pipe_delimited_diagnosis_codes_parsed(self, spark):
        df = make_df(spark, [("C005","M001","P001","MEDICAL",300.0,datetime(2024,1,15),datetime(2024,1,15),"submitted","J18.9|J96.0|Z87.891","99213|71046")])
        dx = parse_code_arrays(df).collect()[0]["diagnosis_codes"]
        assert "J18.9" in dx and "J96.0" in dx and len(dx) == 3

    def test_null_codes_remain_null(self, spark):
        df = make_df(spark, [("C006","M001","P001","MEDICAL",300.0,datetime(2024,1,15),datetime(2024,1,15),"submitted",None,None)])
        assert parse_code_arrays(df).collect()[0]["diagnosis_codes"] is None

    def test_duplicate_codes_deduplicated(self, spark):
        df = make_df(spark, [("C007","M001","P001","MEDICAL",300.0,datetime(2024,1,15),datetime(2024,1,15),"submitted","J18.9|J18.9|J96.0",None)])
        dx = parse_code_arrays(df).collect()[0]["diagnosis_codes"]
        assert dx.count("J18.9") == 1

class TestDeduplication:
    def test_latest_record_kept(self, spark):
        rows = [
            ("C010","M001","P001","MEDICAL",500.0,datetime(2024,1,15),datetime(2024,1,15,8,0),"submitted",None,None),
            ("C010","M001","P001","MEDICAL",550.0,datetime(2024,1,15),datetime(2024,1,15,9,0),"adjusted",None,None),
        ]
        result = deduplicate(make_df(spark, rows)).collect()
        assert len(result) == 1 and result[0]["claim_amount"] == 550.0

    def test_unique_claims_unaffected(self, spark):
        rows = [
            ("C011","M001","P001","MEDICAL",100.0,datetime(2024,1,15),datetime(2024,1,15,8),"submitted",None,None),
            ("C012","M002","P001","PHARMACY",20.0,datetime(2024,1,15),datetime(2024,1,15,8),"submitted",None,None),
        ]
        assert deduplicate(make_df(spark, rows)).count() == 2

class TestValidationQuarantine:
    def test_null_claim_id_quarantined(self, spark):
        rows = [
            (None,"M001","P001","MEDICAL",200.0,datetime(2024,1,15),datetime(2024,1,15,8),"submitted",None,None),
            ("C020","M002","P001","MEDICAL",300.0,datetime(2024,1,15),datetime(2024,1,15,8),"submitted",None,None),
        ]
        valid, quarantine = split_valid_quarantine(make_df(spark, rows))
        assert valid.count() == 1 and quarantine.count() == 1

    def test_negative_amount_quarantined(self, spark):
        rows = [("C021","M001","P001","MEDICAL",-50.0,datetime(2024,1,15),datetime(2024,1,15,8),"submitted",None,None)]
        valid, quarantine = split_valid_quarantine(make_df(spark, rows))
        assert valid.count() == 0 and quarantine.count() == 1

    def test_valid_record_passes(self, spark):
        rows = [("C022","M001","P001","MEDICAL",750.0,datetime(2024,1,15),datetime(2024,1,15,8),"submitted","J18.9","99213")]
        valid, quarantine = split_valid_quarantine(make_df(spark, rows))
        assert valid.count() == 1 and quarantine.count() == 0
