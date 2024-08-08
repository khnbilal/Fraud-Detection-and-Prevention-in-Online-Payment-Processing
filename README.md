# Description:
This repository contains the Data Engineering workload for building a batch-processing fraud detection and prevention system in the online payment processing domain. The project leverages AWS services and Snowflake to create robust data pipelines, ensuring efficient ingestion, transformation, storage, and processing of 1.5 million rows of synthetic payment data.

# Key Features:

- Data Ingestion: Automated data ingestion from CSV files into AWS S3 using AWS Lambda and Glue.
- Data Cleaning & Transformation: ETL pipelines built with AWS Glue to clean and preprocess raw transaction data.
- Data Warehousing: Scalable data storage and management in Snowflake, optimized for fraud detection queries.
- Data Processing & Orchestration: Efficient data pipeline orchestration using Apache Airflow to manage ETL jobs and dependencies.
- Machine Learning Integration: Preparation and extraction of features for model training in Amazon SageMaker.
- Data Security & Compliance: End-to-end data encryption, access management, and auditing to meet security standards.
- Data Quality Assurance: Automated data validation and testing to maintain data integrity throughout the pipeline.

# Technologies Used:
AWS S3, AWS Glue, Snowflake, Amazon SageMaker, Apache Airflow, AWS Lambda, IAM, AWS KMS

# Usage:
This repository provides a complete blueprint for setting up a scalable Data Engineering pipeline that supports fraud detection in online payment systems. It includes scripts, configurations, and documentation to help you deploy, manage, and scale the system.

# Impact:
By automating and optimizing the data processing pipeline, this project enhances the accuracy and efficiency of fraud detection, paving the way for secure and reliable online payment systems
