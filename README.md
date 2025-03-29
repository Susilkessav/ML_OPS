# MLOps Data Pipeline

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Installation & Setup](#installation--setup)
- [Data Pipeline Workflow](#data-pipeline-workflow)
- [Features Implemented](#features-implemented)
- [Running the Pipeline](#running-the-pipeline)
- [Data Versioning](#data-versioning)
- [Testing](#testing)
- [Error Handling & Logging](#error-handling--logging)
- [Reproducibility](#reproducibility)

---

## Overview

This project implements an end-to-end data pipeline for managing and processing Amazon review data using **Apache Airflow**, **DVC (Data Version Control)**, and **PostgreSQL**. The pipeline automates tasks such as data ingestion, preprocessing, transformation, schema validation, and model evaluation.

---

## Project Structure

```
/ML_Ops_Data_Pipeline/
│── dags/                      # Airflow DAGs for orchestration
│── data/                      # Raw and processed data
│── scripts/                   # Python scripts for data processing
│── tests/                     # Unit tests for pipeline components
│── logs/                      # Logs for debugging
│── config/                    # Configuration files
│── .dvc/                      # DVC configuration for data versioning
│── docker-compose.yaml        # Docker configurations
│── requirements.txt           # Python dependencies
│── .env                       # Environment variables
│── README.md                  # Project documentation
```

---

## Installation & Setup

### 1. Clone the repository

```sh
git clone https://github.com/your-username/mlops-data-pipeline.git
cd mlops-data-pipeline
```

### 2. Set up the environment

Install the required dependencies using:

```sh
pip install -r requirements.txt
```

### 3. Configure environment variables

Rename `.env.example` to `.env` and update the following fields:

```env
DB_USER=postgres
DB_PASS=your-password
DB_NAME=airflow
DB_HOST=localhost
DB_PORT=5432
GCS_BUCKET_NAME=your-bucket-name
USE_GCP_SQL=True
```

### 4. Start PostgreSQL & Airflow

```sh
docker-compose up -d
```

---

## Data Pipeline Workflow

The pipeline follows these steps:

1. **Data Ingestion** - Fetch data from cloud storage, APIs, or databases.
2. **Data Preprocessing** - Cleaning, feature engineering, and schema validation.
3. **Data Transformation** - Converting JSON data into structured formats.
4. **Data Storage** - Saving transformed data into PostgreSQL.
5. **Orchestration** - Automating processes using Apache Airflow DAGs.
6. **Data Versioning** - Using DVC to track dataset changes.
7. **Bias Detection & Model Evaluation** - Checking for biases and generating performance metrics.

---

## Features Implemented

- Automated DAG workflow using Airflow\
- Data preprocessing (cleaning, transformation)\
- Data storage in PostgreSQL\
- Data validation and schema enforcement\
- Data version control using DVC\
- Bias detection using data slicing\
- Unit tests for robustness\
- Logging and error handling

---

## Running the Pipeline

### 1. Trigger the DAG in Airflow

To trigger the Amazon reviews pipeline DAG manually, run:

```sh
docker exec -it ml_ops-airflow-worker-1 airflow dags trigger amazon_reviews_pipeline
```

### 2. Check logs for errors

```sh
docker logs ml_ops-airflow-worker-1
```

### 3. List DAG runs

```sh
docker exec -it ml_ops-airflow-worker-1 airflow dags list-runs -d amazon_reviews_pipeline
```

---

## Data Versioning

DVC is used to track dataset changes. To pull the latest data version:

```sh
dvc pull
```

To push new dataset changes:

```sh
dvc add data/raw_data.csv
dvc push
```

---

## Testing

Run unit tests using pytest:

```sh
pytest tests/
```

---

## Error Handling & Logging

- All errors and execution details are logged in the `/logs/` directory.
- Airflow UI provides task-specific logs for debugging.
- Any failed task in the pipeline triggers an alert.

---

## Reproducibility

To reproduce this pipeline on another machine:

1. Clone the repository.
2. Install dependencies (`pip install -r requirements.txt`).
3. Configure `.env` file with correct credentials.
4. Run `docker-compose up -d` to start services.
5. Trigger Airflow DAG to start processing data.

---

