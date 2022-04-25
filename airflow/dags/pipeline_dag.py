import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator, ClusterGenerator

default_args = {
    'depends_on_past': False   
}

CLUSTER_NAME = 'dataproc-cluster'
REGION = os.environ["region"]
PROJECT_ID = os.environ["project"]
BQ_DATASET = os.environ["bq_dataset"]
GS_BUCKET = os.environ["gs_bucket"]
DATA_SRC = os.environ["data_source"]
EVENTS_EL_URI=f'gs://{GS_BUCKET}/pipeline_jobs/events_el_task.py'
CATS_EL_URI=f'gs://{GS_BUCKET}/pipeline_jobs/cats_el_task.py'
PROPS_EL_URI=f'gs://{GS_BUCKET}/pipeline_jobs/props_el_task.py'
ITEMS_FACT_TABLE_TASK_URI=f'gs://{GS_BUCKET}/pipeline_jobs/items_fact_table_task.py'
GS_2_BQ_TASK_URI=f'gs://{GS_BUCKET}/pipeline_jobs/gs_2_bq_task.py'

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    master_machine_type="n1-standard-2",
    master_disk_size= 200,
    worker_machine_type="n1-standard-2",
    worker_disk_size= 200,
    num_workers=2,
).make()

# Define the events EL tasks
EVENTS_EL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": EVENTS_EL_URI,
        "args": [
            f'gs://{DATA_SRC}/events/*',
            f'gs://{GS_BUCKET}/datalake/events/'
        ],
    },
}

# Define the cats EL tasks
CATS_EL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": CATS_EL_URI,
        "args": [
            f'gs://{DATA_SRC}/cats/*',
            f'gs://{GS_BUCKET}/datalake/cats/'
        ],
    },
}

# Define the props EL tasks
PROPS_EL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PROPS_EL_URI,
        "args": [
            f'gs://{DATA_SRC}/props/*',
            f'gs://{GS_BUCKET}/datalake/props/'
        ],
    },
}

ITEMS_FACT_TABLE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": ITEMS_FACT_TABLE_TASK_URI,
        "args": [
            f'gs://{GS_BUCKET}/datalake/cats/*',
            f'gs://{GS_BUCKET}/datalake/events/*',
            f'gs://{GS_BUCKET}/datalake/props/*',
            f'gs://{GS_BUCKET}/datalake/items/'
        ],
    },
}

CREATE_BIGQUERY_TABLES_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GS_2_BQ_TASK_URI,
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
        "args": [
            f'gs://{GS_BUCKET}/datalake/cats/*',
            f'gs://{GS_BUCKET}/datalake/events/*',
            f'gs://{GS_BUCKET}/datalake/props/*',
            f'gs://{GS_BUCKET}/datalake/items/*',
            f'{GS_BUCKET}/data_warehouse/temp/',
            BQ_DATASET,
            'cats',
            'events',
            'props',
            'items',
        ],
    },
}

with DAG(
    'pipeline_dag',
    default_args=default_args,
    catchup = True,
    description='A simple DAG to ingest ecommerce dataset and store it in BigQuery',
    schedule_interval='0 4 * * *',
    start_date = days_ago(1)
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    ingest_events_data = DataprocSubmitJobOperator(
        task_id="ingest_events_data", 
        job=EVENTS_EL_JOB, 
        location=REGION, 
        project_id=PROJECT_ID
    )

    ingest_cats_data = DataprocSubmitJobOperator(
        task_id="ingest_cats_data", 
        job=CATS_EL_JOB, 
        location=REGION, 
        project_id=PROJECT_ID
    )

    ingest_props_data = DataprocSubmitJobOperator(
        task_id="ingest_props_data", 
        job=PROPS_EL_JOB, 
        location=REGION, 
        project_id=PROJECT_ID
    )

    create_items_fact_table = DataprocSubmitJobOperator(
        task_id="create_items_fact_table", 
        job=ITEMS_FACT_TABLE_JOB, 
        location=REGION, 
        project_id=PROJECT_ID
    )

    create_bigquery_tables = DataprocSubmitJobOperator(
        task_id="create_bigquery_tables", 
        job=CREATE_BIGQUERY_TABLES_JOB, 
        location=REGION, 
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    chain(create_cluster, [ingest_events_data, ingest_cats_data, ingest_props_data], create_items_fact_table, create_bigquery_tables, delete_cluster)