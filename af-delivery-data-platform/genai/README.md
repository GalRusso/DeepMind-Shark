# GenAI Data Platform

This project implements a data pipeline for GenAI trends data ingestion and transformation using Databricks Asset Bundles.

## Overview

The GenAI data platform extracts trends data from Google Sheets and processes it through a medallion architecture (Bronze -> Silver) using Databricks Delta Live Tables (DLT).

## Data Sources

The platform ingests data from the following Google Sheets tabs:
- **Bard**: Google Bard AI trends
- **Deepmind**: DeepMind AI trends  
- **Cohere**: Cohere 

**Google Spreadsheet ID**: `1tykiBILQWG3ZHIFAN_zgmDRDSgYU-mv8PxZnC-r4aN8`

## Architecture

### Data Ingestion
- **Tool**: Google Sheets API via `gspread` Python library
- **Filtering**: Spark/Pandas for incremental load based on `date_uploaded` column
- **Storage**: S3 via Databricks Unity Catalog volumes
- **Ingestion Types**:
  - **Incremental**: Today and yesterday data
  - **Full Refresh**: All data (no filter)
  - **Backfill**: Data based on provided dates

### Data Transformation
- **Tool**: Databricks Delta Live Tables (DLT)
- **Architecture**: Medallion (Bronze -> Silver)
  - **Landing (Bronze)**: Raw JSON data from S3
  - **Cleansed (Silver)**: Standardized and deduplicated data

## Project Structure

```
genai/
├── src/
│   ├── data_ingestion/
│   │   ├── gspread2s3_utils.py          # Core ingestion utility
│   │   ├── bard_ingestion.py            # Bard data ingestion
│   │   ├── deepmind_ingestion.py       # Deepmind data ingestion
│   │   ├── cohere_translation_1_ingestion.py
│   │   ├── cohere_translation_2_ingestion.py
│   │   ├── cohere_unique_1_ingestion.py
│   │   ├── cohere_unique_2_ingestion.py
│   │   └── requirements.txt             # Python dependencies
│   └── data_transformation/
│       ├── dlt_genai_bard/              # Bard DLT pipeline
│       ├── dlt_genai_deepmind/          # Deepmind DLT pipeline
│       ├── dlt_genai_cohere_translation_1/
│       ├── dlt_genai_cohere_translation_2/
│       ├── dlt_genai_cohere_unique_1/
│       └── dlt_genai_cohere_unique_2/
├── resources/
│   ├── bard.job.yml                     # Bard job configuration
│   ├── deepmind.job.yml                 # Deepmind job configuration
│   ├── cohere_translation_1.job.yml
│   ├── cohere_translation_2.job.yml
│   ├── cohere_unique_1.job.yml
│   └── cohere_unique_2.job.yml
├── databricks.yml                       # Asset bundle configuration
├── pyproject.toml                       # Python project configuration
└── README.md                            # This file
```

## Configuration

### Environment Variables
- **Google Service Account**: `dbutils.secrets.get(scope="hoangl", key="gdrive-sa")`
- **Catalog**: `af_delivery_dev` (dev) / `af_delivery_prod` (prod)

### Job Parameters
Each job supports the following parameters:
- `run_type`: "incremental", "full_refresh", or "backfill"
- `backfill_dates`: Comma-separated dates in YYYY-MM-DD format (required for backfill)

## Usage

### Deploy the Asset Bundle
```bash
databricks bundle deploy
```

### Run Individual Jobs
```bash
# Run Bard job with incremental load
databricks bundle run bard_job

# Run Deepmind job with backfill
databricks bundle run deepmind_job --parameters run_type=backfill --parameters backfill_dates="2024-01-15,2024-01-16"

# Run Cohere Unique 2 job with incremental load
databricks bundle run cohere_unique_2_job

# Run Cohere Translation 2 job with incremental load
databricks bundle run cohere_translation_2_job

# Run Cohere Unique 1 job with incremental load
databricks bundle run cohere_unique_1_job
```

### Run All Jobs
```bash
# Run all GenAI jobs
databricks bundle run
```

## Data Quality

### Landing Layer Expectations
- `valid_date_uploaded_not_empty`: Ensures date_uploaded column is not null
- `valid_trend_name_not_empty`: Ensures trend_name column is not null

### Cleansed Layer Processing
- Data standardization (trimming, case normalization)
- Deduplication based on trend_name and ingestion_timestamp
- Metadata addition (processing_date, data_source)

## Monitoring

Jobs are scheduled to run daily at 6:00 AM Asia/Ho_Chi_Minh timezone with:
- 2-hour timeout for the entire job
- 30-minute timeout for ingestion tasks
- 1-hour timeout for DLT pipeline tasks
- 2 retry attempts with 1-minute intervals

## Dependencies

### Python Packages
- `pandas>=2.0.0`
- `google-api-python-client>=2.0.0`
- `google-auth>=2.0.0`
- `google-auth-oauthlib>=1.0.0`
- `google-auth-httplib2>=0.1.0`
- `pyspark>=3.4.0`
- `databricks-sdk>=0.8.0`

### Databricks Runtime
- Spark 13.3.x-scala2.12
- Photon enabled
- Delta Lake optimizations enabled

## Author

hoangl@activefence.com