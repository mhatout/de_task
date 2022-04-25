import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, from_unixtime

source_path = (sys.argv[1])
output_path = (sys.argv[2])

spark = SparkSession.builder \
    .master('yarn') \
    .appName('ingest_data') \
    .getOrCreate()  

# Set events DataFrame schema
events_schema = types.StructType([
    types.StructField("timestamp", types.LongType(), True),
    types.StructField("visitorid", types.IntegerType(), True),
    types.StructField("event", types.StringType(), True),
    types.StructField("itemid", types.IntegerType(), True),
    types.StructField("transactionid", types.IntegerType(), True),
])

# Load data from Source.
df = spark.read \
    .option("header", "true") \
    .schema(events_schema) \
    .csv(source_path)
  
df = df \
    .withColumn('timestamp', from_unixtime((col('timestamp')/1000)).cast(types.TimestampType())) 

# Saving the data to DataLake
df \
    .repartition(24) \
    .write \
    .mode('overwrite') \
    .parquet(output_path)
           
