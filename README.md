# Real-Time Fraud Analytics Platform

A Databricks-based fraud analytics platform built using a Medallion Architecture (Bronze, Silver, Gold) on top of the IBM Synthetic Credit Card Transactions dataset.

The platform demonstrates enterprise data engineering practices including data ingestion, transformation, enrichment, fraud analytics, and business-ready analytical marts using Delta Lake and PySpark.

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

## Dataset Characteristics

- ~24M+ transaction records
- 2,000 customers
- Multiple card products
- Merchant and geographic attributes
- Fraud indicators
  
## Roadmap

### Phase 1 (Completed)
- Bronze/Silver/Gold Medallion Architecture
- Customer Risk Analytics
- Fraud Analytics Marts

### Phase 2 (Planned)
- Streaming Fraud Detection
- Databricks Lakeflow Pipelines
- Data Quality Framework
- MLflow Integration

### Phase 3 (Planned)
- FastAPI Analytics Service
- OpenAI / Claude Explainability Layer
- Docker Deployment
- GitHub Actions CI/CD
## Status

Phase 1 Complete
