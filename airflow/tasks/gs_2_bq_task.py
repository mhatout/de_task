import sys
from pyspark.sql import SparkSession

cats_dataset = (sys.argv[1])
events_dataset = (sys.argv[2])
props_dataset = (sys.argv[3])
items_dataset = (sys.argv[4])
bq_temp_bucket = (sys.argv[5])
dataset = (sys.argv[6])
cats_table = (sys.argv[7])
events_table = (sys.argv[8])
props_table = (sys.argv[9])
items_table = (sys.argv[10])

spark = SparkSession.builder \
    .master('yarn') \
    .appName('create_bigquery_table') \
    .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
spark.conf.set("temporaryGcsBucket", bq_temp_bucket)    

# Load data from Datalake.
cats_df = spark.read \
    .parquet(cats_dataset)

events_df = spark.read \
    .parquet(events_dataset)

props_df = spark.read \
    .parquet(props_dataset)

items_df = spark.read \
    .parquet(items_dataset)

# Write the data to Bigquery
cats_df \
    .write.format('bigquery') \
    .mode('overwrite') \
    .option('dataset', dataset) \
    .option('table', cats_table) \
    .save()    

events_df \
    .write.format('bigquery') \
    .mode('overwrite') \
    .option('dataset', dataset) \
    .option('table', events_table) \
    .save()  

props_df \
    .write.format('bigquery') \
    .mode('overwrite') \
    .option('dataset', dataset) \
    .option('table', props_table) \
    .save()  

items_df \
    .write.format('bigquery') \
    .mode('overwrite') \
    .option('dataset', dataset) \
    .option('table', items_table) \
    .save()  