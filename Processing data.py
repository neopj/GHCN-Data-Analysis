
# Processing Q3
# Q3-a 
# indext the first two character of ID, and assign value into new columns

temp = F.trim(F.substring(F.col('ID'),1,2)).cast(StringType())

stations_new = (
    stations_df
    .withColumn("COUNTRYCODE",temp )
    .withColumnRenamed("GSN FLAG","GSNFLAG")
    .withColumnRenamed("HCN/CRN FLAG","HCN/CRNFLAG")
    .withColumnRenamed("WMO ID","WMOID")
    .withColumnRenamed("NAME","STATIONNAME")
)   
stations_new.show(5,False)


# Q3-b
#left join station and countries on country code.
join_station_country = (
     stations_new
     .join(
     countries_df.select(col("CODE").alias("COUNTRYCODE"),col("NAME").alias("COUNTRYNAME")),
     on = "COUNTRYCODE",
     how = "left"
     )
)

join_station_country.show(5, False)

# Q3-c
#left join station and states on state


station_country_states = (
    join_station_country
    .join(
    states_df.select(col("CODE").alias("STATE"),col("NAME").alias("STATENAME")),
    on = "STATE",
    how = "left"
    )
)

station_country_states.show(5, False)

# Q3-d
# the last year and first year of elements for each stations
 year_element = (
    inventory_df
    .select(["ID","FIRSTYEAR","LASTERYEAR"])
    .groupBy("ID")
    .agg(
      F.min("FIRSTYEAR").alias("FIRSTYEAR"),
      F.max("LASTERYEAR").alias("LASTERYEAR")
    )
 )

year_element.show(10,False)

# the count of different elements has each stations collected overall.
distinct_element = (
    inventory_df  
    .select(["ID","ELEMENT"])
    .groupBy("ID")
    .agg(
        F.countDistinct("ELEMENT").alias("DISTINCTCOUNT")
    )
)
distinct_element.show(10, False)

# define core_string contains all core elements
core_string = ["PRCP","SNOW","SNWD","TMAX","TMIN"]

# the number of core elements and other elments for each stations
number_core_other_element = (
       inventory_df
       .select(["ID","ELEMENT"])
       .groupBy("ID")
       .agg(
       F.count(when(col("ELEMENT").isin(core_string),True)).alias("CORECOUNT")
       )
       .join(
       distinct_element.select("ID","DISTINCTCOUNT"),
       on = "ID",
       how = "left"
       )
       .withColumn("OTHERCOUNT", F.col("DISTINCTCOUNT") - F.col("CORECOUNT"))       
     
)
number_core_other_element.show(10,False)

# the number of stations have all core elements
five_core_element= (
    number_core_other_element
    .select("ID","CORECOUNT")
    .where(F.col("CORECOUNT") == 5)
)
    
five_core_element.show(10, False)
five_core_element.count()

# stations only collect precipitation
precipitation_element= (
     inventory_df
     .select(["ID","ELEMENT"])
     .where(F.col("ELEMENT") == "PRCP")
     .join(
     number_core_other_element.select(["ID","CORECOUNT","OTHERCOUNT"]),
     on = "ID",
     how = "left")
     .filter((col("CORECOUNT") == 1) & (col("OTHERCOUNT") == 0))
    )
    
precipitation_element.show()
precipitation_element.count()

# Q3-e
# left join stations and number_core_other_element
temp_inventory = (
      number_core_other_element
      .join(
         year_element.select("*"),
         on = "ID",
         how = "left"
      )
)
temp_inventory.show(5,False)

stations2 = (
    station_country_states
    .select("*")
    .join(
    temp_inventory.select("*"),
    on = "ID",
    how = "left"
    )
)
stations2.show(5,False)
 
# write join_stations_count_inventory as parquet file on HDFS
stations2.write.save('hdfs:///user/jpe116/outputs/ghcnd/stations',format='parquet', mode='overwrite')
hdfs dfs -ls ///user/jpe116/outputs/ghcnd

# Q3-f
# left join 1000 rows of daily.
station_daily = (
    daily2017
    .select("*")
    .join(
    stations2.select("*"),
    on = "ID",
    how = "left"
    )
)
station_daily.show(10,False)

# if any subset of daily that are not in stations at all.
count_null = (
     station_daily
     .select("ID")
     .filter(trim(col("ID")) == "")
)
count_null.count()
