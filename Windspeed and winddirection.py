#Q1


# read station from my output files of hdfs
basePath = "hdfs:///user/jpe116/outputs/ghcnd/stations/"
stations2 = (
     spark.read.option("basePath", basePath).parquet(basePath + "part-*")
    
)

# get the all stations ID of New Zealand.
USA = (
   stations2
   .select("ID","COUNTRYNAME","COUNTRYCODE")
   .filter(col("COUNTRYNAME").like("%United States%"))
)

USA.show(5, False)
USA.count() # 57227

# obtain the Average daily wind direction and wind speed of all stations in NewZealand.
wind_USA = (
    daily_all
    .select("ID","DATE","ELEMENT","VALUE")
    .filter(col("ELEMENT").isin(["AWDR","AWND"]))
    .withColumn("YEAR", F.trim(F.substring(col('DATE'),1,4)).cast(StringType()))
    .join(
       USA,
       on = "ID",
       how = "inner")
    )

wind_USA.cache()
wind_USA.show()

# count the number of observations
wind_USA.count() # 8319925

# the years covered by the observations.
wind_USA_year = (
    wind_USA
    .groupBy("COUNTRYNAME")
    .agg(
      F.min("YEAR"),
      F.max("YEAR")
    )
)
wind_USA_year.show()

# save to parquer file
wind_USA.write.save('hdfs:///user/jpe116/outputs/ghcnd/wind_USA',format='parquet', mode='overwrite')
wind_USA.write.format("csv").save("hdfs:///user/jpe116/ouputs/ghcnd/wind_USA.csv")

# double check if the output was saved
hdfs dfs -ls ///user/jpe116/outputs/ghcnd

# copy the file from hdfs to local home directory
hdfs dfs -copyToLocal /user/jpe116/ouputs/ghcnd/wind_USA ~/wind_USA

# check
cd ~/wind_USA
ls


#Q2
# fail any quality assurance check
fail_mark = ["D","G","I","K","L","M","N","O","R","S","T","W","X"]
fail_quality_check = (
      daily_all
      .filter(col("QUALITY FLAG").isin(fail_mark))
)
fail_quality_check.show(10,False)
fail_quality_check.count()


#  still use the wind_USA to do further research.


