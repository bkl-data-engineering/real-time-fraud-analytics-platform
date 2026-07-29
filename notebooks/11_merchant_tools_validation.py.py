# Databricks notebook source
import importlib
import sys

SRC_ROOT = "/Workspace/real-time-fraud-analytics-platform/src"

if SRC_ROOT not in sys.path:
  sys.path.append(SRC_ROOT)
  
importlib.invalidate_caches()

from agentic_fraud.services.spark_service import SparkService
from agentic_fraud.services.merchant_service import MerchantService
from agentic_fraud.tools import create_merchant_tools

spark_service = SparkService(spark=spark)
merchant_service = MerchantService(
    spark_service=spark_service
)

merchant_tools = create_merchant_tools(
    merchant_service=merchant_service
)
"""
for merchant_tool in merchant_tools:
    print("=" * 80)
    print("NAME:")
    print(merchant_tool.name)

    print("\nDESCRIPTION:")
    print(merchant_tool.description)

    print("\nARGUMENT SCHEMA:")
    print(merchant_tool.args_schema.model_json_schema())
"""

profile_tool = next(
tool
for tool in merchant_tools
    if tool.name == "get_merchant_risk_profile"
)

profile_result = profile_tool.invoke(
    {
        "merchant_name": "4552887027432897467",
        "merchant_city": "Oakland",
        "merchant_state": "CA",
        "mcc": 3596,
    }
)

print(profile_result)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT
            merchant_name,
            merchant_city,
            merchant_state,
            mcc,
            total_transactions,
            fraud_transactions,
            fraud_rate
        FROM fraud_platform.gold.merchant_risk_profile
        where merchant_state = 'CA'
        ORDER BY total_transactions DESC
        LIMIT 20
        """
    )
)