import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, count

cats_path = (sys.argv[1])
events_path = (sys.argv[2])
props_path = (sys.argv[3])
output_path = (sys.argv[4])

spark = SparkSession.builder \
    .master('yarn') \
    .appName('ingest_data') \
    .getOrCreate()  

# Load CATS from Datalake.
cats_df = spark.read \
    .parquet(cats_path)

# Load EVENTS from Datalake.
events_df = spark.read \
    .parquet(events_path)

# Load PROPERTIES from Datalake.
props_df = spark.read \
    .parquet(props_path)

# Join the dataframes.
# Create a new dataframe with item ids as a starting point.
itemid_df = props_df \
    .select('itemid') \
    .dropDuplicates()     

# Add the category ids to the item ids.
props_df.registerTempTable('props_df')

query= """
SELECT 
    props_df.itemid,
    CAST(props_df.value AS int)
FROM
    props_df
INNER JOIN 
    (SELECT itemid, MAX(date) AS MaxDate
    FROM props_df
    WHERE property == 'categoryid'
    GROUP BY 1) groupedprops
ON props_df.itemid = groupedprops.itemid 
AND props_df.date = groupedprops.MaxDate    
WHERE
    property == 'categoryid'  
"""

items_cats_df = spark.sql(query)

items_after_cats_df = itemid_df \
    .join(items_cats_df, 'itemid') \
    .withColumnRenamed('value', 'categoryid')  

# Add the parent ids to the dataframe.
items_after_pcats_df = items_after_cats_df \
    .join(cats_df, 'categoryid', 'left')

# Add the events count to the dataframe.    
agg_items_events_df = events_df.groupBy(['itemid','event']).agg(count('event').alias('event_count')).sort('itemid')
items_views_df = agg_items_events_df.where(agg_items_events_df['event'] == 'view')
items_addtocart_df = agg_items_events_df.where(agg_items_events_df['event'] == 'addtocart')
items_purchased_df = agg_items_events_df.where(agg_items_events_df['event'] == 'transaction')

items_after_events_df = items_after_pcats_df \
    .join(items_views_df, 'itemid', 'left') \
    .withColumnRenamed('event_count', 'views')\
    .withColumn("views" ,col("views").cast(types.IntegerType())) \
    .drop('event') \
    .fillna({'views':'0'}) \
    .join(items_addtocart_df, 'itemid', 'left') \
    .withColumnRenamed('event_count', 'added_to_cart')\
    .withColumn("added_to_cart" ,col("added_to_cart").cast(types.IntegerType())) \
    .drop('event') \
    .fillna({'added_to_cart':'0'}) \
    .join(items_purchased_df, 'itemid', 'left') \
    .withColumnRenamed('event_count', 'purchased')\
    .withColumn("purchased" ,col("purchased").cast(types.IntegerType())) \
    .drop('event') \
    .fillna({'purchased':'0'})

# Add avaiablity status to the dataframe.
query= """
SELECT 
    props_df.itemid,
    CAST(props_df.value AS int) avaliable
FROM
    props_df
INNER JOIN 
    (SELECT itemid, MAX(date) AS MaxDate
    FROM props_df
    WHERE property == 'available'
    GROUP BY 1) groupedprops
ON props_df.itemid = groupedprops.itemid 
AND props_df.date = groupedprops.MaxDate    
WHERE
    property == 'available'  
"""

items_aval_df = spark.sql(query)

items_df = items_after_events_df \
    .join(items_aval_df, 'itemid')

# Saving the data to DataLake
items_df \
    .repartition(24) \
    .write \
    .mode('overwrite') \
    .parquet(output_path)
           
