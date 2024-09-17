# ELT with Airflow, Google Cloud Platform (GCS, Bigquery), and Data Build Tool (DBT)
Data Engineer: A lesson for understanding ELT concepts.


## **About the Porject**
This project implements a robust Extract, Load, Transform (ELT) pipeline that integrates data from multiple sources into a centralized data warehouse on Google Cloud Platform (GCP). The pipeline is designed to handle data from CSV files, MySQL databases, and API endpoints, providing a comprehensive solution for data integration and analysis.

## **Architecture Overview**
![architecture](assets/ELT%20Architecture.png)

## **Key Features**
1. Multi Source Data Extraction : Seamlessly extract data from : CSV, MySQL Databases, API
2. Cloud-Based Data Warehouse : Utilize Google Cloud Platform (GCP) for scalable and secure data storage
3. Orchestration: Employs Apache Airflow for efficient pipeline scheduling and management
4. Data Transformation : Implement dbt(data build tool) for modular and version-controlled data transformation
5. Data Visualization: Integrates with Looker Studio for creating insightful dashboard and reports

## **Technology Stack**
1. Python
2. Airflow
3. Google Cloud Storage (GCS)
4. Google Bigquery
5. Data Build Tool (DBT)
6. Google Looker Studio 

## **Assets Screnshoots** 
## Olympics Dashboard

![architecture](assets/Olympic%20Dashboard%202024.png)

## Olympics Pipeline Airflow

![architecture](assets/Pipeline%20Depedencies.png)

## List of Dag Airflow Webserver
![architecture](assets/List%20of%20Dag.png)

## Airflow Variables
![architecture](assets/Airflow%20Variables.png)

## GCP Connection ID 
![architecture](assets/GCP%20Connection%20ID.png)

## Bigquery Dataset
![architecture](assets/Bigquery%20Dataset.png)

## Google Cloud Storage (GCS)
![architecture](assets/Cloud%20Storage%20Warehouse.png)