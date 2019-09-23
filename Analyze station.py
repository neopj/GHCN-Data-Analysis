# Analysis 
# Q1

# increase resource up to 4 executors, 2 cores per executor,4GB of executor memory,and 4GB of master memory
# fg back to pyspark, ctrl + z stop and go to hdfs.
start_pyspark_shell -m4 -e2 -c2 -w4

# Q1-a
# the number of total stations
stations2.filter(col("ID").isNotNull()).count()


# stations have been actived in 2017
stations2.filter((col("FIRSTYEAR") <= '2017') & (col("LASTERYEAR") >= '2017')).count()


# the number of stations in GSN
stations2.filter(trim(col("GSNFLAG")) == 'GSN').count()

# the number of stations in HCN
stations2.filter(col("HCN/CRNFLAG").like('%HCN%')).count()

# the number of stations in CRN
stations2.filter(col("HCN/CRNFLAG").like('%CRN%')).count()

# stations that are in more than one networks
stations2.filter((col("HCN/CRNFLAG").like('%CRN%')) & (trim(col("GSNFLAG")) == 'GSN')).count()
stations2.filter((col("HCN/CRNFLAG").like('%HCN%')) & (trim(col("GSNFLAG")) == 'GSN')).count()

# Q1-b
# count the number of stations of each country
countries2 = (
    stations2
    .select("ID","COUNTRYCODE","COUNTRYNAME")
    .filter(trim(col("COUNTRYCODE")) != "" )
    .groupBy("COUNTRYCODE")
    .agg(
    F.count(col("ID"))
    )
    .withColumnRenamed("count(ID)","STATIONCOUNT")
)
countries2.show(5,False)

# save output into hdfs
countries2.write.save('hdfs:///user/jpe116/outputs/ghcnd/countries', format='parquet', mode='overwrite')
# double check if the file is saved
hdfs dfs -ls ///user/jpe116/outputs/ghcnd


# count the number of stations of each states
states2 = (
    stations2
    .select("ID","STATE","STATENAME")
    .filter(trim(col("STATE")) != "")
    .groupBy("STATE")
    .agg(
        F.count(col("ID"))
    )
    .withColumnRenamed("count(ID)","STATIONCOUNT")
)
    
states2.show(5,False)

# save output into hdfs
states2.write.save('hdfs:///user/jpe116/outputs/ghcnd/states', format='parquet', mode='overwrite')
# double check if the file is saved
hdfs dfs -ls ///user/jpe116/outputs/ghcnd

# Q1-c
# the number of the stations in the southern Hemisphere only.
station_southern_hemisphere = (
    stations2
    .select("ID","LATITUDE")
    .filter(col("LATITUDE") < 0)
)
station_southern_hemisphere.show(5,False)
station_southern_hemisphere.count()

# the number of the stations in the territories of the United States around the world.
station_USA = (
    stations2
    .select("ID","COUNTRYNAME")
    .filter(col("COUNTRYNAME").like("%United States%"))
)
station_USA.show(20,False)
station_USA.count()


  

