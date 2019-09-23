# Q4-a
# count the row number of whole year in daily 
daily_all=(
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/*.*")
)

daily_all.count()

#Q4-b
# get the subset of observations including the five core elements
core_string = ["PRCP","SNOW","SNWD","TMAX","TMIN"]
core_daily = (
    daily_all
    .select("ID","ELEMENT")
    .filter(col("ELEMENT").isin(core_string))
    .groupBy("ELEMENT")
    .agg(
     F.count(col("ELEMENT"))
    )
    .orderBy(col("count(ELEMENT)"),ascending = False)
 )
   
core_daily.show()

# Q4-c
# get the number of observations of each station including TMAX and TMIN
# group them by date to get the observation at the same observation time
max_min = (
    daily_all
    .select("ID","ELEMENT","DATE")
    .filter(col("ELEMENT").isin(["TMAX","TMIN"]))
    .groupBy("ID","DATE")
    .agg(
       F.collect_set("ELEMENT").alias("Max_Min")
    )
)
max_min.show(20,False)

# get the number of observations of TMIN do not have a corresponding observation of TMAX
only_min = (
    max_min
    .filter((F.array_contains("Max_Min","TMIN")) & (~F.array_contains("Max_Min","TMAX"))
     )
)
only_min.cache()
only_min.show(5,False)
only_min.count()

# get the number of different stations
stations_only_min = only_min.dropDuplicates(['ID'])
stations_only_min.count()


# belong to GSN, HCN or CRN
TMIN_network = (
      stations_only_min
      .join(
        stations2.select("ID","GSNFLAG","HCN/CRNFLAG"),
        on = "ID",
        how = "left"
      )
      .filter((trim(col("GSNFLAG")) != "") | (trim(col("HCN/CRNFLAG")) != ""))
 ).count()
 
 
 # Q4-d
 # summaries of observation of TMIN and TMAX for all stations in New Zealand.
 daily_NZ = (
     stations2
     .filter(col("COUNTRYCODE") == 'NZ')
     .join(
      daily_all.filter(col("ELEMENT").isin(["TMAX","TMIN"])),
      on = "ID",
      how = "inner"
     )
 )

# extract year from DATE
# get the number of year are covered by the observations.
daily_NZ2 = (
    daily_NZ
    .withColumn("YEAR", F.trim(F.substring(col('DATE'),1,4)).cast(StringType()))
    .withColumnRenamed("MEASUREMENT FLAG", "MEASUREMENTFLAG")
    .withColumnRenamed("QUALITY FLAG", "QUALITYFLAG")
    .withColumnRenamed("SOURCE FLAG", "SOURCEFLAG")
    .withColumnRenamed("OBSERVATION TIME", "OBSERVATIONTIME")
)

daily_NZ2.cache()
daily_NZ2.show(5,False)
daily_NZ2.count()

# get the earliest date and latest date of each observation
date_nz = (
    daily_NZ2
    .groupBy("COUNTRYNAME")
    .agg(
      F.min("YEAR"),
      F.max("YEAR")
    )
)

date_nz.show()


# save to parquer file
daily_NZ2.write.save('hdfs:///user/jpe116/outputs/ghcnd/daily_NZ',format='parquet', mode='overwrite')
# daily_NZ2.write.format("csv").save("hdfs:///user/jpe116/ouputs/ghcnd/dialy_NZ2.csv")

# double check if the output was saved
hdfs dfs -ls ///user/jpe116/outputs/ghcnd

# copy the file from hdfs to local home directory
hdfs dfs -copyToLocal /user/jpe116/outputs/ghcnd/daily_NZ ~/daily_NZ

# check daily_NZ
cd ~/daily_NZ
ls

# count the number of rows
 cat *.csv | wc -l
 


# Q4-e 

# compute the average rainfall in each year for each country
average_rainfall= (
     daily_all
     .filter(F.col("ELEMENT") == "PRCP")
     .withColumn("YEAR", F.trim(F.substring(col('DATE'),1,4)).cast(StringType()))
     .join(
     stations2,
     on = "ID",
     how = "left")
     .groupBy("YEAR","COUNTRYNAME","COUNTRYCODE")
     .agg(
       F.avg("VALUE").alias("AVG_RAINFALL")
     )
     .sort(col("AVG_RAINFALL"),ascending = False)
     )
average_rainfall.cache()
average_rainfall.show()

# save the output into hdfs
average_rainfall.write.save("hdfs:///user/jpe116/outputs/ghcnd/average_rainfall", format = "parquet", mode = "overwrite")

hdfs dfs -ls ///user/jpe116/outputs/ghcnd

# cumulative_rainfall for each country
cummu_rainfall_country = (
	average_rainfall
	.groupby(col("COUNTRYCODE"), col("COUNTRYNAME"))
	.agg(
       F.avg("AVG_RAINFALL").alias("AVG_RAINFALL"))
    .sort(col("AVG_RAINFALL"),ascending = False)
	)
cummu_rainfall_country.show(5,False)


cummu_rainfall_country.write.format('csv').save("hdfs:///user/jpe116/ouputs/ghcnd/cummu_rainfall_country.csv")
hdfs dfs -copyToLocal /user/jpe116/ouputs/ghcnd/cummu_rainfall_country ~/cummu_rainfall_country.csv








     
     
     
     