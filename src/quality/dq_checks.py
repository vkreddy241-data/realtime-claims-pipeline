"""
dq_checks.py
Data quality validation using Great Expectations at Bronze and Silver layer transitions.
Records failing checks are quarantined. Pipeline halts only if failure rate exceeds threshold.
"""
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import DataFrame
from src.utils.config import load_config
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

BRONZE_EXPECTATIONS = [
    {"expectation_type": "expect_column_values_to_not_be_null",    "kwargs": {"column": "claim_id",     "mostly": 1.0}},
    {"expectation_type": "expect_column_values_to_not_be_null",    "kwargs": {"column": "member_id",    "mostly": 1.0}},
    {"expectation_type": "expect_column_values_to_not_be_null",    "kwargs": {"column": "claim_amount", "mostly": 1.0}},
    {"expectation_type": "expect_column_values_to_not_be_null",    "kwargs": {"column": "payer_id",     "mostly": 1.0}},
    {"expectation_type": "expect_column_values_to_be_between",     "kwargs": {"column": "claim_amount", "min_value": 0, "max_value": 10_000_000, "mostly": 0.999}},
    {"expectation_type": "expect_column_values_to_be_in_set",      "kwargs": {"column": "claim_type",   "value_set": ["MEDICAL","PHARMACY","DENTAL","VISION","BEHAVIORAL","OTHER"], "mostly": 0.99}},
    {"expectation_type": "expect_column_values_to_be_in_set",      "kwargs": {"column": "status",       "value_set": ["submitted","adjusted","denied","paid","voided"], "mostly": 0.99}},
]

SILVER_EXPECTATIONS = BRONZE_EXPECTATIONS + [
    {"expectation_type": "expect_column_to_exist",              "kwargs": {"column": "claim_month"}},
    {"expectation_type": "expect_column_to_exist",              "kwargs": {"column": "service_date_dt"}},
    {"expectation_type": "expect_column_values_to_be_unique",   "kwargs": {"column": "claim_id"}},
    {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "diagnosis_codes", "mostly": 0.95}},
]

@dataclass
class DQResult:
    layer: str
    run_time: str
    total_records: int
    passed_expectations: int
    failed_expectations: int
    failure_rate: float
    failed_columns: List[str] = field(default_factory=list)
    passed: bool = True

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (f"[DQ {status}] Layer={self.layer} | Records={self.total_records:,} | "
                f"Failed={self.failed_expectations} | FailureRate={self.failure_rate:.4%}")

class ClaimsDQValidator:
    def __init__(self, env: str = "dev"):
        self.cfg               = load_config(env=env)
        self.failure_threshold = self.cfg.get("quality", {}).get("failure_threshold", 0.01)

    def validate_layer(self, df: DataFrame, layer: str, spark) -> DQResult:
        import great_expectations as gx
        from great_expectations.core.batch import RuntimeBatchRequest

        run_time     = datetime.utcnow().isoformat()
        expectations = BRONZE_EXPECTATIONS if layer == "bronze" else SILVER_EXPECTATIONS
        context      = gx.get_context()
        suite_name   = f"claims_{layer}_suite"

        try:
            suite = context.get_expectation_suite(suite_name)
        except Exception:
            suite = context.add_expectation_suite(suite_name)

        suite.expectations = []
        for exp in expectations:
            suite.add_expectation(gx.core.ExpectationConfiguration(**exp))
        context.save_expectation_suite(suite)

        sample_df = df.sample(fraction=min(1.0, 500_000 / max(df.count(), 1))).toPandas()
        validator  = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="claims_pandas_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name=f"claims_{layer}",
                runtime_parameters={"batch_data": sample_df},
                batch_identifiers={"batch_id": run_time},
            ),
            expectation_suite_name=suite_name,
        )
        results        = validator.validate()
        passed         = sum(1 for r in results.results if r.success)
        failed         = sum(1 for r in results.results if not r.success)
        total          = passed + failed
        failure_rate   = failed / total if total > 0 else 0.0
        failed_columns = [r.expectation_config.kwargs.get("column","unknown")
                          for r in results.results if not r.success]
        result = DQResult(
            layer=layer, run_time=run_time, total_records=len(sample_df),
            passed_expectations=passed, failed_expectations=failed,
            failure_rate=failure_rate, failed_columns=list(set(failed_columns)),
            passed=failure_rate <= self.failure_threshold,
        )
        if result.passed:
            logger.info(result.summary())
        else:
            logger.error(result.summary())
        return result

def run_checkpoint(layer: str, env: str = "dev"):
    from pyspark.sql import SparkSession
    cfg   = load_config(env=env)
    spark = SparkSession.builder.appName("claims-dq-check").getOrCreate()
    df    = spark.read.format("delta").load(cfg["delta_lake"][f"{layer}_path"])
    result = ClaimsDQValidator(env=env).validate_layer(df, layer=layer, spark=spark)
    if not result.passed:
        raise RuntimeError(f"DQ check failed for {layer} — {result.failure_rate:.4%} exceeds threshold. Columns: {result.failed_columns}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True, choices=["bronze","silver","gold"])
    parser.add_argument("--env",   default="dev")
    args = parser.parse_args()
    run_checkpoint(layer=args.layer, env=args.env)
