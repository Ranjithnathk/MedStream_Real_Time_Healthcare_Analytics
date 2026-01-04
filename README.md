# MedStream - Real-time Healthcare Analytics (Data Engineering Project)

## Overview
**MedStream** is an end-to-end **real-time healthcare data engineering and analytics platform** built on Microsoft Azure.  
It ingests **streaming patient encounter events** and **batch clinical & financial datasets**, processes them using a **Lakehouse (Medallion) architecture**, and delivers **analytics-ready dashboards in Power BI**.

The platform enables near-real-time operational visibility into:
- Patient encounters
- Department utilization
- Claims and payments
- Payer coverage trends

---

## Business Problem
Healthcare organizations often struggle with fragmented encounter and claims data spread across multiple systems, leading to delayed reporting and limited operational visibility.  
MedStream addresses this by enabling **real-time ingestion, scalable processing, and self-service analytics**.

---

## Architecture Overview
MedStream follows a **hybrid streaming + batch Lakehouse architecture**.

![System Architecture Diagram](https://github.com/Ranjithnathk/MedStream_Real_Time_Healthcare_Analytics/blob/main/docs/MedStream_Architecture_Diagram.png)

---

## Technology Stack

| Layer | Technology |
|------|-----------|
| Streaming Ingestion | Python, Azure Event Hubs (Kafka-compatible) |
| Batch Ingestion | Azure Data Factory |
| Processing | Azure Databricks (Apache Spark) |
| Storage | Azure Data Lake Storage Gen2 |
| SQL Analytics | Azure Synapse Serverless SQL |
| Visualization | Power BI |
| Dataset | Synthea Synthetic Healthcare Data |

---

## Data Sources

**Synthea Patients Records Data:** https://synthea.mitre.org/

**Link:** https://mitre.box.com/shared/static/aw9po06ypfb9hrau4jamtvtz0e5ziucz.zip

### Streaming
- Patient encounter events
- Simulated using a **Python-based producer**
- Published to **Azure Event Hubs**

### Batch
- Patients
- Conditions
- Organizations
- Claims
- Claims Transactions

---

## Data Model & Layers

### Bronze (Raw)
- Raw streaming JSON events
- Raw batch CSV files
- Immutable landing zone

### Silver (Cleaned & Conformed)
- Data cleansing & validation
- Standardized schemas
- Dimensions & fact tables
- Historical snapshots via batch refreshes

### Gold (Analytics-Ready)
- Business-level aggregations
- Optimized for reporting
- Parquet exports for Power BI

**Gold Datasets**
- `encounters_by_department`
- `encounters_by_org_month`
- `payer_coverage_summary`

---

## Power BI Dashboards
Power BI dashboards provide:
- Encounter volume by department
- Monthly encounter & claim trends by organization
- Claim cost vs payments
- Payer coverage and concentration analysis

Power BI consumes **analytics-ready Parquet datasets directly from ADLS Gen2** for high performance and scalability.

### Dashboard Screenshots

![Department Analytics](https://github.com/Ranjithnathk/MedStream_Real_Time_Healthcare_Analytics/blob/main/powerbi/dashboard_screenshots/dashboard_department_analytics.png)

![Organization Monthly Trends](https://github.com/Ranjithnathk/MedStream_Real_Time_Healthcare_Analytics/blob/main/powerbi/dashboard_screenshots/dashboard_org_monthly_trends.png)

![Payer Coverage](https://github.com/Ranjithnathk/MedStream_Real_Time_Healthcare_Analytics/blob/main/powerbi/dashboard_screenshots/dashboard_payer_coverage.png)

---

## How to Run This Project

### Prerequisites
- Azure Subscription
- Azure Databricks Workspace
- Azure Event Hubs (Kafka enabled)
- Azure Data Lake Storage Gen2
- Azure Data Factory
- Azure Synapse Analytics (Serverless SQL)
- Power BI Desktop
- Python 3.8+

---

### Step 1: Set Up Azure Resources
1. Create **ADLS Gen2** with containers:
   - `bronze`
   - `silver`
   - `gold`
2. Create **Azure Event Hubs** (Kafka-compatible)
3. Create **Azure Databricks workspace**
4. Create **Azure Data Factory**
5. Create **Azure Synapse workspace (Serverless SQL)**

---

### Step 2: Streaming Ingestion
1. Place Synthea `patients.csv` and `encounters.csv` locally
2. Configure Event Hub connection in:
   ```bash
   synthea_encounter_producer.py
3. Run the producer:
```bash
python synthea_encounter_producer.py
```
4. Encounter events are published to Event Hub in real time

---

### Step 3: Databricks Streaming Pipeline
Run notebooks in order:
1. **01_bronze_rawdata.ipynb**
    - Reads from Event Hub
    - Writes raw data to Bronze
2. **02_silver_cleandata.ipynb**
    - Cleans & standardizes streaming data
    - Writes to Silver

---

### Step 4: Batch Ingestion (ADF)
1. Upload batch CSVs to Azure Storage
2. Configure ADF Copy pipelines
3. Load batch data into Bronze

**Notebook: 03_bronze_batch_reference_load.ipynb**

---

### Step 5: Silver & Gold Processing
Run notebooks:
1. **04_silver_dimensions.ipynb**
2. **05_silver_fact_encounters_enriched.ipynb**
3. **06_gold_marts_analytics.ipynb**

---

### Step 6: Export for Power BI
**Run: 07_powerbi_exports.ipynb**

This exports Parquet datasets to:
```bash
gold/powerbi/
```

---

### Step 7: Power BI
1. Open Power BI Desktop
2. Get Data → Azure Data Lake Storage Gen2
3. Use:
```bash
https://<storage-account>.dfs.core.windows.net/gold/powerbi
```
4. Load Parquet datasets
5. Build dashboards
6. Publish to Power BI Service

---

## Repository Structure
```bash
MedStream_Real_Time_Healthcare_Analytics/
│
├── databricks/
│   ├── 01_bronze_rawdata.ipynb
│   ├── 02_silver_cleandata.ipynb
│   ├── 03_bronze_batch_reference_load.ipynb
│   ├── 04_silver_dimensions.ipynb
│   ├── 05_silver_fact_encounters_enriched.ipynb
│   ├── 06_gold_marts_analytics.ipynb
│   └── 07_powerbi_exports.ipynb
│
├── simulator/
│   └── synthea_encounter_producer.py
│
├── powerbi/
│   ├── MedStream_Healthcare_Analytics.pbix
│   └── dashboard_screenshots/
│
├── architecture/
│   └── medstream_architecture.png
│
├── docs/
│   └── MedStream_Client_Requirements.pdf
│
├── data/
│   └── synthea_data/
│       └── *.csv
│
├── sqlpool_queries/
│   ├── 01_create_gold_views.sql
│   └── 02_powerbi_access.sql
│
├── requirements.txt
│
└── README.md

```

---

## Future Enhancements
- Implement SCD Type 2 for dimensions
- Add data quality metrics dashboard
- Enable alerting & anomaly detection
- Introduce CI/CD for Databricks notebooks
- Secure secrets using Azure Key Vault

---

## Author

Ranjithnath Karunanidhi
