# Data Pipeline with Airflow

## Project Description
- I built an etl pipeline with airflow using python. I defined a star schema data model that consists of 4 dimensional models and a fact table focused on a particular analytical need. The etl loads data from a s3 bucket into a redshift data warehouse for analytics.

## Project Dataset
I used the popular pagila dataset for this project.

## Schema Design
### Dimensional Tables
  1. Dates
  2. Customer
  3. Movie
  4. Store

### Fact Tables
  1. Sales Fact

## Project Structure
- The project contains 2 folders and 3 files
    1. dags folder: This contains the dag file.
    2. plugins folder: this contains the helper folders and operators used in building the airflow data pipeline
    3. create_tables.py: this file builds the tables in the redshift data warehouse.
    4. create_tables.sql: this contains the sql queries to create the tables.
    5. dwh.cfg: contains the configuration codes for our aws infrastructures.


## Airflow Structure
### Tree view
![](https://github.com/geewynn/pagila/blob/master/airflow_pipeline/images/airflow%20tree.jpg)
### Graph View
![](https://github.com/geewynn/pagila/blob/master/airflow_pipeline/images/airflow%20graph.jpg)

## Setting up the project
   1. Create an IAM user and take note of the secret key and access key.
   2. Create aws redshift role: Create a redshift role under IAM.
   3. Create Redshift clusters: First you need to create a redshift cluster, with you preferred configuration details and connects the cluster to the redshift IAM role.
   4. Airflow connection: Create a new airflow connection on airflow containing details of your redshift connections including the database info.
   5. Create a new variable containing your bucket name.
   6. Add configuration details to the dwh.cfg file.

## Run the Project
  1. Run create_tables.py to create the data warehouse tables.
  2. Go to your airflow UI and run the dag.
 


