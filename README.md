Real-Time Data Processing with Airflow, Kafka, and Spark

Project Overview

This project demonstrates a real-time data processing pipeline using Airflow, Kafka, and Spark. It fetches data from the Random User Generator API
 and processes it from raw messy data to clean, structured data stored in Cassandra for further analytics.

The pipeline involves:

Data Extraction: Airflow DAGs extract data from the API periodically.

Staging Area: Raw data in JSON format is stored in PostgreSQL as a staging area.

Real-Time Streaming: Kafka streams the data in real time to the Spark cluster.

Data Cleaning & Transformation: Spark jobs process, clean, and transform the data.

Final Storage: Cleaned and transformed data is stored in Cassandra for analytics and downstream applications.

Architecture

The architecture consists of:

Airflow: Orchestrates the pipeline and manages scheduling using DAGs.

Kafka: Provides a high-throughput, real-time data streaming layer.

Spark: Performs data cleaning and transformation on streaming data.

PostgreSQL: Acts as a staging area for raw JSON data.

Cassandra: Stores cleaned and transformed data for analytics and reporting.

Features

Real-time data ingestion from APIs

Messy JSON data handling and staging

Stream processing with Kafka and Spark

Cleaned and structured data storage in Cassandra

Fully automated pipeline using Airflow DAGs

Technologies Used

Python

Apache Airflow

Apache Kafka

Apache Spark

PostgreSQL

Cassandra
