<!-- omit in toc -->
## Languages
[![python](https://img.shields.io/badge/python-3.11-d6123c?color=white&labelColor=d6123c&logo=python&logoColor=white)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-d6123c?color=white&labelColor=d6123c)](https://en.wikipedia.org/wiki/SQL)

<!-- omit in toc -->
## Frameworks
[![apache-airglow](https://img.shields.io/badge/apache--airflow-2.11.0-d6123c?color=white&labelColor=d6123c&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![pandas](https://img.shields.io/badge/pandas-2.2.3-d6123c?color=white&labelColor=d6123c&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![clickhouse-connect](https://img.shields.io/badge/clickhouse--connect-0.8.17-d6123c?color=white&labelColor=d6123c&logo=clickhouse&logoColor=white)](https://github.com/ClickHouse/clickhouse-connect)
[![minio](https://img.shields.io/badge/minio-7.2.15-d6123c?color=white&labelColor=d6123c&logo=minio&logoColor=white)](https://min.io/)
[![matplotlib](https://img.shields.io/badge/matplotlib-3.10.3-d6123c?color=white&labelColor=d6123c&logo=matplotlib&logoColor=white)](https://matplotlib.org/)
[![seaborn](https://img.shields.io/badge/seaborn-0.13.2-d6123c?color=white&labelColor=d6123c&logo=seaborn&logoColor=white)](https://seaborn.pydata.org/)

<!-- omit in toc -->
## Services
[![clickhouse](https://img.shields.io/badge/clickhouse-d6123c?style=for-the-badge&logo=clickhouse&logoColor=white)](https://clickhouse.com/)

<!-- omit in toc -->
## Table of Contents
- [Introduction](#introduction)
- [Project workflow](#project-workflow)
- [Docker Containers](#docker-containers)
- [Auto-analytic DAGs](#auto-analytic-dags)
  - [Daily DAG](#daily-dag)
  - [Monthly DAG](#monthly-dag)
  - [ClickHouse Archive DAG](#clickhouse-archive-dag)
- [Getting Started](#getting-started)
- [Next Section of the Project](#next-section-of-the-project)

## Introduction
🟢 **This is part 6 of 7 Docker sections in the 🔴 [Supermarket Simulation Project](https://github.com/SerhiiDolhopolov/rossmann_services).**

🔵 [**<- Previous part with API.**](https://github.com/SerhiiDolhopolov/rossmann_api)

## Project workflow
This section contains Airflow DAGs with auto-reports sent to email and archiving ClickHouse data to S3 to reduce the load on the DWH and lower data storage costs.

## Docker Containers
**This Docker section includes:**
  - **Airflow**
    - 🌐 Web interface: 
      - [localhost:1600](http://localhost:1600)
    - Login:
      - [airflow](airflow)
    - Password:
      - [airflow](airflow)
  
## Auto-analytic DAGs
The auto-analytic DAGs have a modular structure with a DAG template. It's easy to add a new diagram to a DAG.

### Daily DAG
The daily DAG has 3 parallel operators for creating diagrams, then an operator for creating a PDF with these diagrams, and then sends the PDF to email and S3.

![daily dag](images/daily_dag.png)
![daily dag email](images/daily_dag_email.png)
![daily dag s3](images/daily_dag_s3.png)

### Monthly DAG
The monthly DAG has 4 parallel operators for creating diagrams, then an operator for creating a PDF with these diagrams, and then sends the PDF to email and S3. Each monthly diagram aggregation is saved in S3 to use for longer analytics because ClickHouse has a TTL of 4 months.

![monthly dag](images/monthly_dag.png)
![monthly dag email](images/monthly_dag_email.png)
![monthly dag report](images/monthly_dag_rep.png)
![monthly dag s3](images/monthly_dag_s3.png)

### ClickHouse Archive DAG
Each month, ClickHouse transactions are archived in S3 because S3 storage is several times cheaper. For archiving, the .parquet file format is used. This format has a high level of compression, columnar data storage, and both ClickHouse and Spark work well with it. The disadvantage is that people can't read the file directly.

![archive dag](images/archive_dag.png)
![archive dag s3](images/archive_dag_s3.png)

## Getting Started
**To start:**
1. Complete all steps in the [main part](https://github.com/SerhiiDolhopolov/rossmann_services).
2. Run the services:
```bash
docker compose up --build
```

## Next Section of the Project

[Rossmann Spark](https://github.com/SerhiiDolhopolov/rossmann_spark)
