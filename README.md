# Reddit ETL Pipeline

This project provides a robust solution for extracting, transforming, and loading (ETL) Reddit data into an Amazon Redshift data warehouse. The pipeline utilizes several modern tools and services for scalability, reliability, and efficiency.

## Architecture

![Blank diagram](https://github.com/user-attachments/assets/6f9600a4-2c59-4acd-844b-8a980604514e)


## Overview

The pipeline automates the extraction of Reddit data using the Reddit API, performs transformations, and loads it into a Redshift data warehouse for analytics and reporting. Key services and tools involved:

- **Apache Airflow**: Orchestration of the ETL workflow.
- **Celery**: Task queue for parallel processing.
- **PostgreSQL**: Temporary storage for intermediate data.
- **Amazon S3**: Data storage for raw and processed data.
- **AWS Glue**: Data transformation and cataloging.
- **Amazon Athena**: Query service for data exploration.
- **Amazon Redshift**: Data warehousing and analytics.

## Features

- Automated data extraction from Reddit API.
- Parallel data processing using Celery and Airflow.
- Scalable data storage on S3 and Redshift.
- Flexible data transformations using AWS Glue.
- Easy querying with Athena for ad-hoc analysis.

## Getting Started

### Prerequisites

- AWS Account with access to Redshift, Glue, Athena, and S3.
- Reddit API credentials.
- Docker (optional for containerized deployment).
- Python 3.7+.

### Installation

1. Clone this repository:
    ```bash
    https://github.com/Prasadmuthyala/Reddit-pipeline.git
    cd reddit-etl-pipeline
    ```

2. Install required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Configure your AWS credentials and Reddit API keys.

### Running the Pipeline

1. Start the Airflow web server:
    ```bash
    airflow webserver -p 8080
    ```

2. Start the Celery worker:
    ```bash
    celery -A airflow_worker worker --loglevel=info
    ```

3. Trigger the ETL DAG in Airflow UI.

## Workflow

1. **Data Extraction**: Reddit data is fetched through the Reddit API.
2. **Data Transformation**: The data is cleaned and structured using AWS Glue.
3. **Data Loading**: Processed data is loaded into Amazon Redshift for analysis.
4. **Querying**: Data can be queried via Amazon Athena for ad-hoc analysis.

## Acknowledgments

A special thank you to [@airscholar](https://github.com/airscholar) for the inspiration and valuable teachings that helped shape this project. Your work was a great learning resource!


