import sys

PROJECT_ROOT = "/Workspace/real-time-fraud-analytics-platform"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import importlib
importlib.reload(sys.modules["pipelines.dq.dq_validator"])
from pipelines.dq.dq_validator import DQValidator


CATALOG = "fraud_platform"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

validator = DQValidator(
    spark=spark,
    catalog=CATALOG,
    silver_schema=SILVER_SCHEMA,
    gold_schema=GOLD_SCHEMA,
)

print("Running data quality validations...")

dq_results_df = validator.run_all_validations()

dq_results_df.show(truncate=False)

validator.write_dq_results(
    dq_results_df,
    table_name="dq_summary",
    mode="append",
)

print("Data quality validation completed successfully.")
