# Spotify Data ETL Pipeline using AWS

![Spotify Data ETL Pipeline](./Spotify_Project/Project_Structure.PNG)

## Project Overview
This project implements a data engineering pipeline for extracting, transforming, and loading (ETL) Spotify data using AWS services. The pipeline is designed to collect data via the Spotify API, process it, and store it for analysis. The architecture leverages AWS Lambda, Amazon S3, AWS Glue, and Amazon Athena for efficient and scalable data management.

## Architecture

### **1. Extract**
- **Spotify API**: Fetches raw music data such as tracks, artists, and playlists.
- **AWS Lambda**: Handles the data extraction process and stores the raw data in an Amazon S3 bucket.
- **Amazon S3 (Raw Data)**: Serves as the landing zone for unprocessed data.

### **2. Transform**
- **AWS Lambda**: Processes and transforms the raw data into a structured format (e.g., JSON or CSV).
- **Amazon S3 (Transformed Data)**: Stores the processed data, ready for cataloging and analysis.

### **3. Load**
- **AWS Glue Crawler**: Scans the transformed data to infer schema and update the AWS Glue Data Catalog.
- **AWS Glue Data Catalog**: Maintains metadata for the datasets.
- **Amazon Athena**: Provides SQL-based query capabilities for analysis.

## Key Features
- **Automated Data Pipeline**: Utilizes AWS Lambda for event-driven automation.
- **Serverless Architecture**: Fully serverless, ensuring scalability and cost-effectiveness.
- **Schema Inference**: Employs AWS Glue Crawler to infer data schema.
- **SQL-Based Analytics**: Enables data exploration and visualization using Amazon Athena.

## How to Run
1. Set up AWS Lambda functions for data extraction and transformation.
2. Configure S3 buckets for raw and processed data.
3. Use AWS Glue to catalog the data and define schemas.
4. Query the processed data using Amazon Athena.

## Tools & Technologies
- **Spotify API**: Source of raw data.
- **AWS Lambda**: For serverless compute.
- **Amazon S3**: For raw and transformed data storage.
- **AWS Glue**: For schema inference and data cataloging.
- **Amazon Athena**: For data querying and analysis.

---

Feel free to explore this project and contribute!
