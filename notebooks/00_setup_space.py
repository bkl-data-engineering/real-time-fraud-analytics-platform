# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS fraud_platform;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS fraud_platform.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS fraud_platform.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS fraud_platform.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS fraud_platform.dq;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS fraud_platform.bronze.raw_files;
# MAGIC CREATE VOLUME IF NOT EXISTS fraud_platform.bronze.micro_batches;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files('/Volumes/fraud_platform/bronze/raw_files/credit_card_transactions-ibm_v2.csv') limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files(
# MAGIC   '/Volumes/fraud_platform/bronze/raw_files/credit_card_transactions-ibm_v2.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true
# MAGIC ) limit 5;

# COMMAND ----------

