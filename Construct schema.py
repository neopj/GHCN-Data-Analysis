# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession,functions as F
from pyspark.sql.types import * 
from pyspark.sql.functions import col, isnan, when, trim


# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Q2-a Define schemas

# daily schema
schema_daily = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", DoubleType(), True),
    StructField("MEASUREMENT FLAG", StringType(), True),
    StructField("QUALITY FLAG", StringType(), True),
    StructField("SOURCE FLAG", StringType(), True),
    StructField("OBSERVATION TIME", TimestampType(), True)
])

# stations schema
schema_stations = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEVATION", DoubleType(), True),
    StructField("STATE", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("GSN FLAG", StringType(), True),
    StructField("HCN/CRN FLAG", StringType(), True),
    StructField("WMO ID", StringType(), True)
    
])

# states schema
schema_states = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True)
    
])

# countries schema
schema_countries = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True)
    
])


#inventory schema
schema_inventory = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("FIRSTYEAR", IntegerType(), True),
    StructField("LASYYEAR", IntegerType(), True),
    
])


# Q2-b load 1000 rows of 2017 daily

daily2017 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
    .limit(1000)
)
daily2017.cache()
daily2017.show(10, False)

# Q2-c load stations

stations = (
     spark.read.format("text")
    .load("hdfs:///data/ghcnd/stations")
    
)
stations_df = stations.select(
    F.trim(F.substring(F.col('Value'),1,11)).alias('ID').cast(StringType()),
    F.trim(F.substring(F.col('Value'),13,8)).alias('LATITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('Value'),22,9)).alias('LONGITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('Value'),32,6)).alias('ELEVATION').cast(DoubleType()),
    F.trim(F.substring(F.col('Value'),39,2)).alias('STATE').cast(StringType()),
    F.trim(F.substring(F.col('Value'),42,30)).alias('NAME').cast(StringType()),
    F.trim(F.substring(F.col('Value'),73,3)).alias('GSN FLAG').cast(StringType()),
    F.trim(F.substring(F.col('Value'),77,3)).alias('HCN/CRN FLAG').cast(StringType()),
    F.trim(F.substring(F.col('Value'),81,5)).alias('WMO ID').cast(StringType())
    
)
stations_df.show(10,False)
stations_df.count()


# load states

states = (
     spark.read.format("text")
    .load("hdfs:///data/ghcnd/states")
    
)


states.show(10, False)
states_df = states.select(
      F.trim(F.substring(F.col('Value'),1,2)).alias('CODE').cast(StringType()),
      F.trim(F.substring(F.col('Value'),4,47)).alias('NAME').cast(StringType())
)
      
states_df.show(10,False)

states_df.count()


# load countries

countries = (
     spark.read.format("text")
    .load("hdfs:///data/ghcnd/countries")
    
)
countries.show(10, False)

countries_df = countries.select(
      F.trim(F.substring(F.col('Value'),1,2)).alias('CODE').cast(StringType()),
      F.trim(F.substring(F.col('Value'),4,47)).alias('NAME').cast(StringType())
)
      
countries_df.show(10,False)

countries_df.count()

      
# load inventory

inventory = (
     spark.read.format("text")
    .load("hdfs:///data/ghcnd/inventory")
    
)
inventory.show(10, False)

inventory_df = inventory.select(
    F.trim(F.substring(F.col('Value'),1,11)).alias('ID').cast(StringType()),
    F.trim(F.substring(F.col('Value'),13,8)).alias('LATITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('Value'),22,9)).alias('LONGITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('Value'),32,4)).alias('ELEMENT').cast(StringType()),
    F.trim(F.substring(F.col('Value'),37,4)).alias('FIRSTYEAR').cast(IntegerType()),
    F.trim(F.substring(F.col('Value'),42,4)).alias('LASTERYEAR').cast(IntegerType()),
    
)
inventory_df.show(10,False)
inventory_df.count()

# count the null value of WMO ID in Stations
stations_df.filter(trim(col("WMO ID")) == "").count()