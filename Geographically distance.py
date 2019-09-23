# Analysis Q2
# Q2-a
from pyspark.sql.functions import udf


# define a function that computes the geographical distance between two points
from math import radians, cos, sin, asin, sqrt
def compute_distance(lon1,lat1,lon2,lat2):
    lon1,lat1,lon2,lat2 = map(radians,[lon1, lat1,lon2,lat2])  
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = (sin(dlat/2))**2 + cos(lat1) * cos(lat2) * (sin(dlon/2)) **2
    c = 2 * asin(sqrt(a))
    r = 6371
    return round((c*r*1000)/1000,2)
    
    
      
# use udf function to wrap the compute_distance()      
distance_udf = F.udf(compute_distance, DoubleType())    

# test function
test1 = (
   stations2
   .select("ID","LATITUDE","LONGITUDE")
   .withColumnRenamed("ID","ID1")
   .withColumnRenamed("LATITUDE","LATITUDE1")
   .withColumnRenamed("LONGITUDE","LONGITUDE1")
   .filter(col("COUNTRYNAME").like("%United States%"))
)
test1.show(5,False)

test2 = (
    stations2
   .select("ID","LATITUDE","LONGITUDE")
   .withColumnRenamed("ID","ID2")
   .withColumnRenamed("LATITUDE","LATITUDE2")
   .withColumnRenamed("LONGITUDE","LONGITUDE2")
   .filter(col("COUNTRYNAME").like("%United States%"))
)
test2.show(5,False)

test_distance = (
     test1
     .crossJoin(test2)
     .filter(col("ID1") != col("ID2"))
     .withColumn("distance",
            distance_udf("LATITUDE1","LONGITUDE1","LATITUDE2","LONGITUDE2")
    )
 )
 
test_distance.show(5,False)

# Q2-b
# compute the pairwise distances between New Zealand
new_zealand = (
    stations2
    .select("ID","COUNTRYNAME","LATITUDE","LONGITUDE")
    .filter(col("COUNTRYNAME").like("%New Zealand%"))
 )

new_zealand.show(5, False)

station1 = (
    new_zealand
    .select("ID","LATITUDE","LONGITUDE")
    .withColumnRenamed("ID","ID1")
    .withColumnRenamed("LATITUDE","LATITUDE1")
    .withColumnRenamed("LONGITUDE","LONGITUDE1")
)

station2 = (
    new_zealand
    .select("ID","LATITUDE","LONGITUDE")
    .withColumnRenamed("ID","ID2")
    .withColumnRenamed("LATITUDE","LATITUDE2")
    .withColumnRenamed("LONGITUDE","LONGITUDE2")
)

# compute the geographical distance
distance_stations_NZ = (
    station1
    .crossJoin(station2)
    .filter(station1.ID1 != station2.ID2)
    .withColumn("distance",
                distance_udf("LATITUDE1","LONGITUDE1","LATITUDE2","LONGITUDE2")
                )

)
distance_stations_NZ.show(5,False)
        

# find the closest geographically stations in New Zealand
distance_stations_NZ = (
    distance_stations_NZ
    .dropDuplicates(["distance"])
    .orderBy("distance")

)

distance_stations_NZ.show(5,False)

# save the stations distance into hdfs
distance_stations_NZ.write.save('hdfs:///user/jpe116/outputs/ghcnd/distance_stations_',format="parquet",mode="overwrite")
hdfs dfs -ls ///user/jpe116/outputs/ghcnd