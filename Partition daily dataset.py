#Q3
# Q3-a
# show the first couple of files/directories in daily
hdfs dfs -ls /data/ghcnd/daily | head

# show all size of files in daily directory
hdfs dfs -du /data/ghcnd/daily

# the number of blocks are required for the daily climate summaries for the year 2010
 hadoop fsck /data/ghcnd/daily/2010.csv.gz

# the number of blocks are required for the daily climate summaries for the year 2017
 hadoop fsck /data/ghcnd/daily/2017.csv.gz


# Q3-b
# count the row number of year 2017
daily2017 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
)

daily2017.count()


# count the row number of year 2010
daily2010 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2010.csv.gz")
)

daily2010.count()

#Q3-c
# count the row number of year 2010-2015
loadpath = "hdfs:///data/ghcnd/daily/201[0-5].csv.gz"
daily2010_2015 = (
     spark.read.format("com.databricks.spark.csv")
     .option("header","false")
     .option("inferSchema","false")
     .schema(schema_daily)
     .load(loadpath)
)

daily2010_2015.count()


