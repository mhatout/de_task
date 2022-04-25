# ELT_PIPELINE
A data ELT pipeline to ingest datasets consist of eCommerce-related data then perform some cleaning and preprocessing on it and lastly load those datasets into Google Bigquery data warehouse for the data science team to work on. 

## Project Description
This is the end-to-end project to ingest different eCommerce-related datasets and load them into our data lake then build structure tables in Google Bigquery to represent those datasets in structured form for our data science team to be able to access the data easily. In this project, I covered the scope of data engineering (building the data pipeline). I used Terraform to provision my cloud infrastructure, Apache Airflow to orchestrate the workflow of the pipeline, and Apache Spark for data ingestion, preprocessing, and loading.

> I assumed that for proper analysis and machine learning training process to be performed successfully, We would need to create an aggregated fact table for the items containing all the attributes that can be extracted from the dataset. So besides ingesting the data and loading it in our data lake with a proper schema I did some transformation/aggregation on the dataset to create that table (ITEMS table). 

## Objective
  * Extract different datasets from multiple sources.
  * Define a schema for the data and perform some data cleaning on the datasets.
  * Load the data into our data lake in a structured form.
  * Create fact table for the items.
  * Create Bigquery tables to represent the loaded datasets.

## Dataset
Consest of defferent eCommerce-related datasets.

## Tools & Technology
* Terraform (IaC)
* Cloud: Google Cloud Platform (GCP)
  * Managed Cloud Scheduling: Google Composer
  * Managed Proccesing Cluster: Google Dataproc
  * Data Lake: Google Cloud Storage (GCS)
  * Data Warehouse: Google Big Query (GBQ)
* Orchestration: Apache Airflow
* Data Transformation: Apache Spark (Pyspark)
* Scripting Language: Python

## Data Pipeline
I have created a an Airflow DAG that will create a dataproc cluster and run the data run Spark jobs that will perform the entire ETL process. The DAG consists of the following steps:
 * create_dataproc_cluster:
    * Create a DataProc cluster by useing "DataprocCreateClusterOperator" Airflow operator.
 * ingest_data:
    * Load the data from the source and ingest it into a Spark dataframe.
    * Perform some data cleaning, preprocessing.
    * upload the datafram into our data lake (google cloud storage (GCS) in parquet format).
 * create_bigquery_table:
    * gcs_bigquery: create the table in data warehouse, google big query (GBQ) by the parquet files in datalake.
 * delete_dataproc_cluster:
    * Delete the Dataproc cluster after finishing all the jobs submitted to avoid any unnecessary costs.      
<img alt = "image" src = "https://i.ibb.co/zbRdPpM/Screen-Shot-2022-04-24-at-4-21-07-PM.png">

## Reproductivity

# Step 1: Setup the Base Environment <br>
Required Services: <br>
* [Google Cloud Platform](https://console.cloud.google.com/) - register an account with credit card GCP will give you free $300 valid for 3 months
    * Create a service account & download the keys as json file. The json file will be useful for further steps.
    * Enable the API related to the services (Google Compute Engine, Google Cloud Storage, Cloud Composer, Cloud Dataproc & Google Big Query)
    * follow the Local Setup for Terraform and GCP [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp)

# Step 2: Provision your cloud infrastructure useing Terraform<br>
* copy your GCP project name into "project" var in variable.tf.
* run the following commands:
```console
terraform init
```
```console
terraform plan
```
```console
terraform apply -auto-approve
```
> Provisioning the cloud infrastructure may take up to 20 minutes.

# Step 3: Run the data pipeline using Airflow<br>

* Copy the airflow dag file into the airflow/dags folder.
```console
gsutil cp path_to_the_project/de_task/airflow/dags/pipeline_dag.py gs://CLOUD_COMPOSER_BUCKET/dags/
```
> You can find the Airflow dag floder by going to Cloud Composer page and clicking on the "DAGs" tab and you can access Airflow UI web server by clicking on "Airflow" tab.
<img alt = "image" src = "https://i.ibb.co/4VnJcFF/composer-dags.png">

* Copy the Airflow dag tasks into GS bucket.
```console
gsutil -m cp path_to_the_project/de_task/airflow/tasks/* gs://GS_BUCKET_NAME/pipeline_jobs/
```
* From the Airflow UI page triger the pipeline_dag DAG and wait for it to finish.
> This process may take up to 20 minutes.

## Further Improvements:
* Provide The needed data structure based on the model that would be trained on the dataset to be able to perform a complete and proper transformation process on the data.

