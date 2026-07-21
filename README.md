# 🚨 Real-Time Fraud Analytics Platform

A production-style data engineering platform built on **Databricks** to ingest, process, validate, and analyze credit card transactions using both **batch** and **real-time streaming** pipelines.

The project demonstrates modern data engineering practices including **Medallion Architecture**, **Delta Lake**, **PySpark Structured Streaming**, **Unity Catalog**, **Databricks Jobs**, and modular Python design. It demonstrates how financial institutions build scalable analytics platforms that transform raw transaction data into trusted, analytics-ready datasets for fraud analytics, operational reporting, and future AI-driven investigation.

Unlike many portfolio projects that focus solely on machine learning models, this project emphasizes the engineering foundation required for reliable fraud analytics—data ingestion, schema standardization, incremental processing, data quality validation, orchestration, and production-oriented pipeline design.

The platform processes over 24 million historical credit card transactions while also supporting incremental streaming ingestion using PySpark Structured Streaming.

---

## Key Highlights

* Batch and streaming data pipelines implemented using PySpark on Databricks
* Medallion Architecture (Bronze → Silver → Gold)
* Delta Lake tables managed through Unity Catalog
* Incremental processing with PySpark Structured Streaming
* Multi-task Databricks Job orchestration
* Built-in data quality validation framework
* Modular, reusable Python transformers and configuration classes
* Gold-layer analytical data marts for fraud reporting and merchant risk analysis
* Production-oriented project structure with separation of orchestration, transformation, and validation logic

> **Status:** ✅ Complete (Batch + Streaming Pipelines)

---

## Solution Architecture

> **📌 Note:** A high-level architecture diagram illustrating the end-to-end batch and streaming data flow will be added here.

```
Raw Data
    │
    ▼
 Bronze
    │
    ▼
 Silver
    │
    ▼
 Gold
    │
    ▼
 Analytics & Reporting
```

## Business Problem

Financial institutions process millions of credit card transactions every day across merchants, channels, and geographic locations. Before fraud detection models, business intelligence dashboards, or real-time alerting systems can deliver meaningful insights, organizations must first build reliable and scalable data platforms capable of ingesting, validating, transforming, and organizing high-volume transaction data.

Building such platforms presents several engineering challenges:

* Processing both historical and continuously arriving transaction data.
* Standardizing inconsistent schemas from multiple source datasets.
* Enriching transactions with customer, card, and merchant reference data.
* Maintaining trusted, analytics-ready datasets with built-in data quality validation.
* Supporting incremental processing without reprocessing historical data.
* Delivering curated data products that enable downstream fraud analytics and reporting.

This project addresses those challenges by implementing a production-inspired data engineering platform using Databricks and the Medallion Architecture. Rather than focusing on predictive fraud models, the project emphasizes the engineering foundation required to support reliable fraud analytics at scale.

The resulting platform demonstrates how raw financial transaction data can be transformed into trusted analytical datasets that support merchant risk analysis, customer profiling, demographic reporting, and future fraud detection capabilities.

> **Portfolio Focus**
>
> This project intentionally emphasizes production-oriented data engineering practices—including scalable ingestion, incremental processing, orchestration, and data quality—rather than machine learning model development.

## Platform Architecture

The Real-Time Fraud Analytics Platform implements a layered data engineering architecture that transforms raw credit card transaction data into trusted, analytics-ready datasets through a series of modular, reusable processing stages.

The platform supports both **historical batch ingestion** and **incremental streaming ingestion**, allowing the same analytical data model to be populated from either large historical datasets or continuously arriving transaction files.

Key architectural principles include:

* **Medallion Architecture** to progressively improve data quality and usability across Bronze, Silver, and Gold layers.
* **Modular pipeline design** that separates ingestion, transformation, validation, and orchestration into reusable components.
* **Incremental processing** using PySpark Structured Streaming to efficiently process newly arriving transaction data.
* **Data quality validation** integrated into both batch and streaming workflows to ensure trusted analytical outputs.
* **Centralized governance** using Unity Catalog for managed Delta Lake tables.
* **Production-oriented orchestration** through multi-task Databricks Jobs that coordinate the end-to-end streaming workflow.

The resulting Gold layer provides curated analytical datasets that support fraud analytics, merchant risk assessment, customer profiling, and demographic reporting while remaining extensible for future machine learning and AI-driven investigation use cases.

## Architecture Principles

The platform was designed around a set of architectural principles commonly used in modern enterprise data platforms. These principles guided both the technical implementation and the organization of the project.

| Principle                  | Implementation                                                                                                                                                                                      |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Separation of Concerns** | The Medallion Architecture separates raw ingestion (Bronze), standardized transformations (Silver), and business-ready analytics (Gold), making each layer independently maintainable and testable. |
| **Modularity**             | Pipeline logic is organized into reusable Python transformers, configuration classes, and orchestration notebooks, reducing duplication and simplifying future enhancements.                        |
| **Scalability**            | Batch and streaming pipelines share common transformation patterns while Structured Streaming processes only newly arriving transaction files.                                                      |
| **Reliability**            | Streaming checkpoints, Delta Lake transactions, and dedicated validation notebooks help ensure fault-tolerant processing and consistent analytical outputs.                                         |
| **Data Quality**           | Validation is treated as a first-class concern through required field checks, referential integrity validation, and automated verification queries.                                                 |
| **Governance**             | Unity Catalog provides centralized management of Delta tables, schemas, and metadata across Bronze, Silver, and Gold layers.                                                                        |
| **Maintainability**        | The repository separates ingestion, transformation, validation, orchestration, and documentation into clearly defined modules to improve readability and long-term maintainability.                 |
| **Extensibility**          | The modular design allows future integration of machine learning models, REST APIs, AI-powered fraud investigation, monitoring, and CI/CD pipelines without major architectural changes.            |

---

### End-to-End Workflow

The following workflow illustrates how data moves through the platform from ingestion to consumption:

📥 Raw Data Ingestion

   * Historical CSV datasets are loaded into Bronze Delta tables.
   * Incremental transaction files are processed through Structured Streaming.

2. 🥉 Bronze Layer

   * Preserves raw source data with ingestion metadata.
   * Captures lineage information including source file, ingestion timestamp, load type, and record hash.

🥈 Silver Layer

   * Standardizes schemas and data types.
   * Cleanses and enriches transactional data.
   * Joins customer, card, and merchant reference datasets.
   * Produces consistent datasets for downstream analytics.

🥇 Gold Layer

   * Builds curated analytical data marts.
   * Generates merchant risk metrics, customer profiles, demographic summaries, and fraud-oriented analytical views.

✅ Data Quality & Validation

   * Executes automated data quality validations.
   * Confirms referential integrity and required field completeness.
   * Produces validation summaries for operational verification.

📊 Consumption

   * Gold tables are optimized for SQL analytics, dashboards, and future API or AI-driven applications.


## Project Scope

This project intentionally focuses on building the **data engineering foundation** required to support modern fraud analytics rather than implementing predictive fraud models.

The primary objective is to demonstrate production-oriented data engineering practices including:

* Scalable batch and streaming data ingestion
* Medallion Architecture implementation
* Delta Lake-based data management
* Incremental processing with PySpark Structured Streaming
* Modular pipeline design and reusable transformation components
* Data quality validation and operational verification
* Databricks Job orchestration
* Curated analytical data marts for downstream reporting and investigation

The project establishes a trusted analytical data platform upon which advanced capabilities—such as machine learning models, real-time fraud scoring, REST APIs, AI-assisted investigation, and interactive dashboards—can be added with minimal architectural changes.

By intentionally limiting the scope to the data platform itself, the project reflects the responsibilities typically expected of Senior and Lead Data Engineers responsible for designing and operating enterprise-scale analytics pipelines.

## Technology Stack

The platform leverages a modern cloud-native data engineering stack designed to support scalable batch processing, real-time streaming, governed data management, and production-oriented pipeline orchestration.

| Architecture Layer          | Technology                   | Why It Was Selected                                                                                                                                                        |
| --------------------------- | ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Analytics Platform**      | Databricks                   | Provides a unified environment for batch processing, streaming, Delta Lake, notebook development, SQL analytics, and workflow orchestration.                               |
| **Distributed Processing**  | PySpark                      | Enables scalable distributed transformations for large historical datasets and incremental streaming workloads.                                                            |
| **Storage Layer**           | Delta Lake                   | Provides ACID transactions, schema enforcement, efficient MERGE operations, and reliable incremental processing.                                                           |
| **Streaming Engine**        | PySpark Structured Streaming | Processes newly arriving transaction files incrementally while supporting fault-tolerant execution through checkpointing.                                                  |
| **Data Governance**         | Unity Catalog                | Centralizes metadata, schema management, and governed access to Delta tables across Bronze, Silver, and Gold layers.                                                       |
| **Architecture Pattern**    | Medallion Architecture       | Separates raw ingestion, standardized transformations, and business-ready analytics into maintainable data layers.                                                         |
| **Workflow Orchestration**  | Databricks Jobs              | Coordinates the end-to-end streaming workflow using multiple dependent tasks and automated execution.                                                                      |
| **Programming Language**    | Python                       | Supports modular, object-oriented pipeline development with reusable transformers, configuration classes, and validation components.                                       |
| **Version Control**         | Git & GitHub                 | Enables source control, collaboration, version history, and public portfolio presentation.                                                                                 |
| **Development Environment** | Databricks Free Edition      | Provides a cloud-native development environment for implementing, testing, and validating the complete data platform using the community-accessible edition of Databricks. |


## Medallion Architecture

The platform follows the Databricks Medallion Architecture, progressively improving data quality and business value as data moves through each processing layer.

| Layer         | Purpose                      | Key Responsibilities                                                                                                                                                   |
| ------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 🥉 **Bronze** | Raw ingestion                | Ingests historical and streaming transaction files while preserving source data, ingestion metadata, lineage information, and record hashes.                           |
| 🥈 **Silver** | Standardization & Enrichment | Cleanses data, standardizes schemas, performs type conversions, enriches transactions with customer and card information, and prepares trusted datasets for analytics. |
| 🥇 **Gold**   | Business Analytics           | Produces curated analytical data marts including merchant risk metrics, customer profiles, demographic summaries, and fraud-oriented reporting datasets.               |

The layered architecture improves maintainability, isolates responsibilities, simplifies debugging, and enables downstream analytical consumers to work exclusively with trusted business-ready datasets.


## Repository Structure

The repository is organized to separate transformation logic, orchestration, documentation, and supporting utilities. This mirrors the organization commonly found in production data engineering projects and promotes maintainability, reusability, and ease of navigation.

```text
real-time-fraud-analytics-platform/
│
├── docs/                    # Architecture diagrams, design documents, and project documentation
├── jobs/                    # Streaming data generation and orchestration utilities
├── notebooks/               # Databricks orchestration and validation notebooks
├── pipelines/               # Modular Bronze, Silver, and Gold transformation pipelines
├── src/                     # Shared application components and configuration
├── tests/                   # Future automated testing framework
├── README.md                # Project documentation
├── requirements.txt         # Python dependencies
└── LICENSE                  # Open-source license
```

Each directory has a clearly defined responsibility, allowing transformation logic, orchestration workflows, validation notebooks, and documentation to evolve independently while keeping the overall project organized and easy to extend.


## Batch Pipeline

The batch pipeline processes the complete historical transaction dataset and establishes the analytical foundation of the platform.

### Processing Flow

1. Load raw CSV datasets into Bronze Delta tables.
2. Capture ingestion metadata and lineage information.
3. Standardize schemas and cleanse data within the Silver layer.
4. Enrich transactions using customer and card reference datasets.
5. Generate curated Gold analytical data marts.
6. Execute automated data quality validations.
7. Validate outputs using dedicated verification notebooks.

The batch implementation demonstrates a production-oriented ETL workflow that transforms raw transactional data into governed analytical datasets while maintaining clear separation between ingestion, transformation, enrichment, and reporting.


## Streaming Pipeline

The streaming implementation extends the batch architecture by processing newly arriving transaction files incrementally using PySpark Structured Streaming.

Rather than reprocessing historical datasets, the platform continuously ingests micro-batches of transaction data, applies the same transformation principles established by the batch pipeline, and incrementally updates Gold analytical tables.

### Streaming Workflow

1. Simulated transaction files are generated into the Bronze landing zone.
2. Auto Loader detects newly arriving files.
3. Bronze streaming tables capture raw transactions with ingestion metadata.
4. Streaming Silver transformations standardize and enrich incoming records.
5. Gold streaming transformations incrementally update analytical data marts.
6. Automated validation notebooks verify successful processing.

The streaming workflow is orchestrated using a multi-task Databricks Job and demonstrates production-style incremental processing, checkpoint management, and reusable transformation logic.


## Data Quality Framework

Reliable analytics begin with trusted data. Rather than treating validation as a post-processing activity, this platform integrates data quality verification directly into both the batch and streaming workflows.

The validation framework confirms that data remains complete, consistent, and analytically reliable before it is consumed by downstream reporting or future fraud detection applications.

### Validation Categories

| Validation Area               | Purpose                                                                                                                |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **Required Field Validation** | Verifies that mandatory business attributes are populated before downstream processing.                                |
| **Referential Integrity**     | Confirms relationships between transactions, cards, and customer reference datasets remain valid after transformation. |
| **Analytical Validation**     | Verifies that Gold data marts contain expected business aggregations and metrics.                                      |
| **Pipeline Verification**     | Dedicated validation notebooks confirm successful execution of batch and streaming pipelines.                          |

By embedding validation throughout the platform, data quality becomes an integral part of the engineering workflow rather than a separate operational activity.


## Gold Analytical Data Marts

The Gold layer delivers business-ready analytical datasets optimized for reporting, investigation, and future fraud analytics. Each data mart represents a curated view of transactional data designed to answer a specific business question.

| Gold Data Mart            | Business Purpose                                                                                                        |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Customer Card Profile** | Consolidated customer and card information supporting customer-centric analytics and reporting.                         |
| **Fraud Transactions**    | Curated transaction dataset highlighting records relevant for fraud investigation and downstream analytical processing. |
| **Merchant Risk Profile** | Aggregated merchant-level metrics supporting risk assessment and transaction monitoring.                                |
| **Fraud by State**        | Geographic aggregation of transaction activity for regional fraud analysis.                                             |
| **Fraud by Age Group**    | Demographic summaries enabling age-based behavioral analysis.                                                           |
| **Fraud by Income Band**  | Income-based analytical views supporting customer segmentation and fraud trend analysis.                                |

These analytical datasets provide a trusted business layer that can be queried directly by reporting tools, dashboards, SQL analytics, or future machine learning applications.


## Workflow Orchestration

The streaming platform is automated using a multi-task Databricks Job that coordinates the complete end-to-end processing workflow.

Each task performs a specific responsibility while maintaining clear separation between data generation, ingestion, transformation, and validation.

### Job Workflow

| Task                          | Responsibility                                                                                    |
| ----------------------------- | ------------------------------------------------------------------------------------------------- |
| **Generate Streaming Data**   | Creates incremental transaction files that simulate continuously arriving financial transactions. |
| **Streaming Bronze Pipeline** | Ingests newly arriving files into Bronze Delta tables while capturing ingestion metadata.         |
| **Streaming Silver Pipeline** | Standardizes schemas, cleanses data, and enriches incoming transactions.                          |
| **Streaming Gold Pipeline**   | Incrementally updates analytical Gold data marts using trusted Silver datasets.                   |
| **Validation Pipeline**       | Executes verification queries confirming successful processing and expected analytical outputs.   |

The modular orchestration approach mirrors production scheduling patterns and allows individual processing stages to evolve independently while maintaining a predictable end-to-end workflow.


## Key Engineering Achievements

This project demonstrates the design and implementation of a production-oriented data engineering platform using modern Databricks technologies and architectural best practices.

### Highlights

* Designed and implemented complete **batch and streaming data pipelines** using PySpark and Delta Lake.
* Applied the **Medallion Architecture** to separate ingestion, transformation, and analytical processing into maintainable data layers.
* Built reusable, modular Python components to separate transformation logic from orchestration and configuration.
* Implemented incremental streaming workflows using **PySpark Structured Streaming** and **Databricks Auto Loader**.
* Automated the end-to-end streaming workflow using a **multi-task Databricks Job**.
* Integrated data quality validation into both batch and streaming pipelines through dedicated verification workflows.
* Produced curated Gold analytical data marts supporting merchant risk analysis, customer profiling, demographic reporting, and fraud analytics.
* Designed the platform for future extensibility including machine learning, REST APIs, dashboards, and AI-assisted fraud investigation.

Collectively, these capabilities demonstrate many of the architectural patterns and operational practices expected when building modern cloud-native analytics platforms.


## Engineering Decisions

The platform intentionally adopts several architectural patterns commonly used in enterprise data engineering environments. Each design decision was made to improve maintainability, scalability, operational reliability, and future extensibility.

| Engineering Decision       | Rationale                                                                                                                                               |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Medallion Architecture** | Separates raw ingestion, standardized transformations, and business analytics into clearly defined processing layers with independent responsibilities. |
| **Delta Lake**             | Provides ACID transactions, schema enforcement, and efficient incremental updates while simplifying data reliability.                                   |
| **Structured Streaming**   | Processes only newly arriving transaction data, avoiding unnecessary reprocessing of historical datasets.                                               |
| **Modular Python Design**  | Encapsulates transformation logic into reusable components that can be shared across batch and streaming implementations.                               |
| **Unity Catalog**          | Centralizes governance, metadata management, and organization of analytical datasets.                                                                   |
| **Databricks Jobs**        | Separates orchestration from transformation logic, enabling automated execution and simplified operational workflows.                                   |
| **Validation Framework**   | Treats data quality as an integral part of the pipeline rather than a post-processing activity.                                                         |
| **Analytical Gold Layer**  | Produces business-ready datasets optimized for reporting while remaining extensible for future machine learning and AI workloads.                       |

These decisions emphasize long-term maintainability and reflect the architectural trade-offs commonly encountered when building production-scale data platforms.


## Project Metrics

The following metrics summarize the overall scale and scope of the platform.

| Metric                            |                                                               Value |
| --------------------------------- | ------------------------------------------------------------------: |
| Historical Transactions Processed |                                                            **24M+** |
| Source Datasets                   |                                                               **4** |
| Medallion Layers                  |                                                               **3** |
| Batch Pipeline                    |                                                               **1** |
| Streaming Pipeline                |                                                               **1** |
| Gold Analytical Data Marts        |                                                               **6** |
| Databricks Job Tasks              |                                                               **5** |
| Data Quality Framework            |                 Integrated throughout batch and streaming workflows |
| Processing Model                  |                                       Batch + Incremental Streaming |
| Repository Organization           | Modular Python architecture with reusable transformation components |

These metrics highlight both the functional capabilities of the platform and the engineering practices applied throughout its implementation.


## Future Enhancements

The current implementation establishes a scalable data engineering foundation designed to support future analytical and operational capabilities with minimal architectural change.

Potential enhancements include:

| Enhancement                   | Business Value                                                                                                                  |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **Real-Time Fraud Scoring**   | Integrate machine learning models to assign fraud risk scores as transactions are processed through the streaming pipeline.     |
| **REST API Layer**            | Expose Gold analytical datasets through FastAPI services for application integration and operational reporting.                 |
| **Interactive Dashboards**    | Visualize merchant risk, customer behavior, transaction trends, and pipeline metrics using BI tools.                            |
| **AI-Assisted Investigation** | Enable natural language exploration of fraud datasets using LLM-powered analytical assistants.                                  |
| **CI/CD Automation**          | Implement automated testing and deployment pipelines for notebooks, Python modules, and Databricks Jobs.                        |
| **Monitoring & Alerting**     | Extend operational observability with pipeline health monitoring, execution metrics, and automated notifications.               |
| **Cloud Deployment**          | Deploy the platform within a production cloud environment using Infrastructure as Code and environment-specific configurations. |

The modular architecture intentionally anticipates these capabilities, allowing the platform to evolve from a portfolio implementation into a production-ready analytics solution.


## Lessons Learned

Building this platform reinforced several engineering principles that extend beyond fraud analytics and are broadly applicable to modern data engineering systems.

* **Well-defined architectural boundaries simplify long-term maintenance.** Separating ingestion, transformation, validation, orchestration, and analytics reduced complexity and allowed each component to evolve independently.

* **Batch and streaming pipelines benefit from shared design patterns.** Reusing transformation logic across both processing models minimized duplication while ensuring consistent analytical outputs.

* **Data quality should be embedded throughout the pipeline.** Treating validation as a core engineering responsibility improved confidence in downstream analytical datasets and simplified troubleshooting.

* **Modularity enables extensibility.** Organizing transformation logic into reusable Python components provides a solid foundation for future enhancements such as machine learning, APIs, dashboards, and AI-assisted analytics.

* **Architecture decisions have long-term operational impact.** Choosing technologies such as Delta Lake, Unity Catalog, Structured Streaming, and Databricks Jobs simplified governance, incremental processing, and workflow automation while preserving flexibility for future growth.

This project demonstrates that successful data platforms are defined not only by the technologies they use, but also by the architectural decisions that make them maintainable, scalable, and adaptable over time.

## Dataset & Acknowledgements

This project uses the **IBM Synthetic Credit Card Transactions** dataset, publicly available on Kaggle and created by **Eric Altman**. The dataset provides a large-scale synthetic representation of credit card transactions and associated reference data, making it well suited for demonstrating modern data engineering architectures and fraud analytics workflows.

**Dataset:**
https://www.kaggle.com/datasets/ealtman2019/credit-card-transactions

### Files Used

* `credit_card_transactions-ibm_v2.csv`
* `sd254_cards.csv`
* `sd254_users.csv`

The dataset contains more than **24 million synthetic credit card transactions**, along with customer demographics, card attributes, merchant information, and fraud labels. Within this project, the data is used to demonstrate:

* Batch and real-time streaming data ingestion
* Medallion Architecture implementation
* Data standardization and enrichment
* Merchant risk analysis
* Customer profiling
* Demographic analytics
* Data quality validation
* Future fraud detection and AI-assisted investigation scenarios

**Acknowledgement**

Special thanks to **Eric Altman** for creating and publishing this dataset for the data engineering and data science community through Kaggle. The dataset provides an excellent foundation for exploring scalable analytics architectures and fraud-related use cases.



## About the Author

This project was designed and implemented by **Bhasker Lakshmikanth** as part of an ongoing professional portfolio focused on modern cloud-native data engineering, real-time analytics, and AI-enabled data platforms.

With more than two decades of software engineering experience—including technical leadership, engineering management, and hands-on platform development—my focus is on building scalable, maintainable, and production-oriented data systems that transform complex business requirements into reliable analytical solutions.

If you'd like to connect or discuss this project:

* **GitHub:** https://github.com/bkl-data-engineering
* **LinkedIn:** https://www.linkedin.com/in/bhasker-lakshmikanth-bl

```

---
