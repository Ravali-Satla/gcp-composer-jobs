# Load GCS to BigQuery DAG

This Airflow DAG (Directed Acyclic Graph) automates the process of loading data from Google Cloud Storage (GCS) into BigQuery.

## Description

The DAG is designed to run periodically and load data from a specific file (`bat.csv`) stored in a GCS bucket  into a BigQuery table. The schema of the data to be loaded is predefined.

## Dependencies

- Airflow
- Google Cloud Storage
- BigQuery

## Author

Ravali Satla.


