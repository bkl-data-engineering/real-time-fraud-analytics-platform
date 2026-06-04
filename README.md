# Real-Time Fraud Analytics Platform

A Databricks-based fraud analytics platform built using a Medallion Architecture.

## Technology Stack

- Databricks Free Edition
- Unity Catalog
- Delta Lake
- PySpark
- Python
- Medallion Architecture

## Architecture

Bronze Layer
- Raw ingestion of users, cards and transactions datasets

Silver Layer
- Data quality checks
- Data standardization
- Type casting
- User enrichment

Gold Layer
- Customer Card Profile
- Fraud Transactions
- Fraud by State
- Fraud by Age Group
- Fraud by Income Band
- Merchant Risk Profile

## Data Flow

Raw Files → Bronze → Silver → Gold

## Dataset

IBM Credit Card Fraud Dataset
This project uses the IBM Synthetic Credit Card Transactions dataset published on Kaggle.

Dataset:
https://www.kaggle.com/datasets/ealtman2019/credit-card-transactions

Files used:
- credit_card_transactions-ibm_v2.csv
- sd254_cards.csv
- sd254_users.csv

The dataset contains millions of synthetic credit card transactions, customer demographics, card attributes, merchant information, and fraud labels. It is used to demonstrate fraud analytics, customer risk profiling, merchant risk scoring, and future real-time fraud detection use cases.

Dataset author: Eric Altman
Platform: Kaggle

## Status

Phase 1 Complete
