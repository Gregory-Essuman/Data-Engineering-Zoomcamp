# Project Documentation

The objective of the project is to create a pipeline which gets data from dataverse.harvard.edu, ingests the data into a data lake, perform some transformations and visualize few aggregations on a dashboard. The 

## Problem Description

Time is an essential commodity in life and managing time is very crucial to efficiency. One aspect of everyday life which consumes most of time is travelling. Due to travelling or commuting being time consuming in itself, people hate to wait or be delayed when about to travel. One of transportation modes which is air travel does not fall behind when it comes to time consumption and occasional delay. As an avid traveller, I seek to analyse past air travel data in the United states from 1987 to 2008 and be informed on which airlines mostly delay in taking off. This would be beneficial to travelers when about to travel.

Source of Data: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi%3A10.7910%2FDVN%2FHG7NV7&version=&q=&fileTypeGroupFacet=%22Unknown%22&fileAccess=&fileTag=%222.+Data%22&fileSortField=&fileSortOrder=#

## Technologies Used

1. Docker
2. Apache Airflow
3. Terraform
4. Google Cloud Platform (GCS, BigQuery, Data Studio)
5. DBT

## Data Pipeline

This project makes use of a one time batch process however it can be applied in situations where processing could be monthly, or anually.

## Data Ingestion and Orchestration (Apache Airflow)

The data was ingested to the data lake (GCS) and data warehouse (Big query) with orchestration from airflow.

## Data Warehouse (Big Query)

The dataset ingested in the data warehouse was partitioned and clustered to enhance query performance.

## Reproduction

1. Create google service account and give proper credentials and permissions.

2. Install python, docker, terraform and git.

3. Clone the codes from git repository and change the codes to the newly created environment.

4. Navigate to terraform directory and run commands to provision infrastructure. Make sure to edit credential details and other variables.

4. To run airflow, navigate to airflow directory and run commands to initialize the containers in docker.

5. Go to localhost 8080 and login with airflow login details
    user - airflow
    password - airflow

6. Trigger the dags to run the pipeline
    first trigger - data_ingestion_gcs_dag.py
    second trigger - external_table_dag_.py

7. Login to google cloud platform and check the ingested data in GCS and Bigquery.