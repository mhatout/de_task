import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, from_unixtime

source_path = (sys.argv[1])
output_path = (sys.argv[2])

spark = SparkSession.builder \
    .master('yarn') \
    .appName('ingest_data') \
    .getOrCreate()  

# Set category_tree DataFrame schema
cats_schema = types.StructType([
    types.StructField("categoryid", types.IntegerType(), True),
    types.StructField("parentid", types.IntegerType(), True)
])

# Load data from Source.
df = spark.read \
    .option("header", "true") \
    .schema(cats_schema) \
    .csv(source_path)
  
# Saving the data to DataLake
df \
    .repartition(4) \
    .write \
    .mode('overwrite') \
    .parquet(output_path)
           
