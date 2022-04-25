import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, from_unixtime, to_date

source_path = (sys.argv[1])
output_path = (sys.argv[2])

spark = SparkSession.builder \
    .master('yarn') \
    .appName('ingest_data') \
    .getOrCreate()  

# Set props DataFrame schema
props_schema = types.StructType([
    types.StructField("timestamp", types.LongType(), True),
    types.StructField("itemid", types.IntegerType(), True),
    types.StructField("property", types.StringType(), True),
    types.StructField("value", types.StringType(), True)
])

# Load data from Source.
df = spark.read \
    .option("header", "true") \
    .schema(props_schema) \
    .csv(source_path)

df = df \
    .withColumn('date', to_date(from_unixtime((col('timestamp')/1000)))) \
    .drop('timestamp')

# Saving the data to DataLake
df \
    .repartition(24) \
    .write \
    .mode('overwrite') \
    .parquet(output_path)
           
