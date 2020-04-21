#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init('/home/dimitris/spark-2.4.5-bin-hadoop2.7')
from pyspark import SparkConf,SparkContext


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('hotelsandres1tauran1').getOrCreate()


# In[3]:


from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType,DoubleType
from pyspark.sql import functions as F
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import col,min, max,concat, col, lit,abs,broadcast
from datetime import datetime
import timeit
import numpy as np
import pandas as pd
import random 
import string
from datetime import datetime
import timeit


# In[4]:


def get_distance(longit_a, latit_a, longit_b, latit_b):
  # Transform to radians
  longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a,  latit_a, longit_b, latit_b])
  dist_longit = longit_b - longit_a
  dist_latit = latit_b - latit_a
  # Calculate area
  area = sin(dist_latit/2)**2 + cos(latit_a) * cos(latit_b) * sin(dist_longit/2)**2
  # Calculate the central angle
  central_angle = 2 * asin(sqrt(area))
  radius = 6371
  # Calculate Distance
  distance = central_angle * radius
  return distance
udf_get_distance = F.udf(get_distance)


# In[5]:


def pyspark_distance_calculator1(distance):

  #Timer
  elapsed_time_df = timeit.default_timer()
  

  #Loading the first Dataset
  sdf1 = spark.createDataFrame(df1)
  sdf1 = sdf1.withColumn("Type", lit("FirstOne"))

  #Loading the second Dataset
  sdf2 = spark.createDataFrame(df2)
  sdf2 = sdf2.withColumn("Type", lit("SecondOne"))

  #elapsed time to read dataframes
  elapsed_time_df = timeit.default_timer() - elapsed_time_df

  #elapsed time to read grid
  elapsed_time_findgrid = timeit.default_timer()

  #finding the point which is nearest to north or south
  place1=sdf1.groupBy().min('lat', 'lon')
  place2=sdf1.groupBy().max('lat', 'lon')
  place3=sdf2.groupBy().min('lat', 'lon')
  place4=sdf2.groupBy().max('lat', 'lon')
  appended = place1.union(place2)
  appended = appended.union(place3)
  appended = appended.union(place4)
  appended = appended.select(col("min(lat)").alias("minlat"), col("min(lon)").alias("minlot"))
  appended = appended.withColumn('Value',abs(appended.minlat).cast(DoubleType()))
  appended = appended.withColumn('minlot1',appended.minlot.cast(DoubleType()))
  appended= appended.groupBy().max('minlot1', 'Value')
  appended = appended.select(col("max(minlot1)").alias("lon"), col("max(Value)").alias("lat"))

  #Creating Points by decimal number in order to find the distance 
  havers10 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+10, appended.lat).cast(DoubleType())).withColumn("Delta", lit(0.1))
  havers2 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+2, appended.lat).cast(DoubleType())).withColumn("Delta", lit(2))
  havers1 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+1, appended.lat).cast(DoubleType())).withColumn("Delta", lit(1))
  havers01 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+0.1, appended.lat).cast(DoubleType())).withColumn("Delta", lit(10))
  havers001 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+0.01, appended.lat).cast(DoubleType())).withColumn("Delta", lit(100))
  havers0001 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+0.001, appended.lat).cast(DoubleType())).withColumn("Delta", lit(1000))
  havers00001 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+0.0001, appended.lat).cast(DoubleType())).withColumn("Delta", lit(10000))
  appended = havers10.union(havers1)
  appended = appended.union(havers2)
  appended = appended.union(havers01)
  appended = appended.union(havers001)
  appended = appended.union(havers0001)
  appended = appended.union(havers00001)
  appended=appended.withColumn("Deltafromdistance",appended.ABS_DISTANCE-distance)
  mindelta = appended.filter(appended.Deltafromdistance>0)
  mindelta= mindelta.groupBy().min('Deltafromdistance')
  mindelta = mindelta.select(col("min(Deltafromdistance)").alias("minimun"))
  left_join = appended.join(mindelta, appended.Deltafromdistance == mindelta.minimun,how='inner')
  left_join = left_join.select('Delta')

  #elapsed time to read grid
  elapsed_time_findgrid  = timeit.default_timer() - elapsed_time_findgrid
  
  #elapsed time to merge points
  elapsed_time_merge_points = timeit.default_timer()

  #find the nearest points and duplicate points
  try:
    multiplier= left_join.collect()[0][0]
  except:
    multiplier= 90000

  if multiplier==2:


    sdf2 = sdf2.withColumn("latG", (sdf2["lat"]).cast(IntegerType())).withColumn("lonG", (sdf2["lon"]).cast(IntegerType()))
    sdf2 = sdf2.withColumn("latG", sdf2["latG"].cast(StringType())).withColumn("lonG", sdf2["lonG"].cast(StringType()))
    sdf2 = sdf2.withColumn("tempkey", concat(sdf2["latG"],lit("|") ))
    sdf2 = sdf2.withColumn("key", concat(sdf2["tempkey"],sdf2["lonG"]) )
    
    sdf1= sdf1.withColumn("latG", (sdf1["lat"]).cast(IntegerType()))
    sdf1 = sdf1.withColumn("lonG", (sdf1["lon"]).cast(IntegerType()))
    sdfnew = sdf1
    for lati in range(-2,3):
      for loni in range(-2,3):
        if lati !=0 or loni!=0:
          sdf1000000 = sdfnew.withColumn("latG", (sdfnew["latG"]+lati).cast(StringType())).withColumn("lonG", (sdfnew["lonG"]+loni).cast(StringType()))
          sdf1 = sdf1.union(sdf1000000)
    sdf1 = sdf1.withColumn("tempkey", concat(sdf1["latG"],lit("|") ))
    sdf1 = sdf1.withColumn("key", concat(sdf1["tempkey"],sdf1["lonG"])).repartition(20,'key')
    sdf2 = sdf2.select(col("name").alias("hotelname"), col("lat").alias("hlat"), col("lon").alias("hlon"),col("key").alias("key"), col("Type").alias("Type"),col("key").alias("hkey")).repartition(9,'hkey')
    sdf1= sdf1.join(sdf2, sdf1.key == sdf2.hkey,how='inner')


  elif multiplier<=10000:

    sdf2=sdf2.withColumn("Delta",lit(multiplier))
    sdf1=sdf1.withColumn("Delta",lit(multiplier))

    sdf2 = sdf2.withColumn("latG", (sdf2["Delta"]*sdf2["lat"]).cast(IntegerType())).withColumn("lonG", (sdf2["Delta"]*sdf2["lon"]).cast(IntegerType()))
    sdf2 = sdf2.withColumn("latG", sdf2["latG"].cast(StringType())).withColumn("lonG", sdf2["lonG"].cast(StringType()))
    sdf2 = sdf2.withColumn("tempkey", concat(sdf2["latG"],lit("|") ))
    sdf2 = sdf2.withColumn("key", concat(sdf2["tempkey"],sdf2["lonG"]) )

    sdf1= sdf1.withColumn("latG", (sdf1["Delta"]*sdf1["lat"]).cast(IntegerType()))
    sdf1 = sdf1.withColumn("lonG", (sdf1["Delta"]*sdf1["lon"]).cast(IntegerType()))
    sdfnew=sdf1
    for lati in range(-1,2):
      for loni in range(-1,2):
        if lati !=0 or loni!=0:
          sdf1000000 = sdfnew.withColumn("latG", (sdfnew["latG"]+lati).cast(StringType())).withColumn("lonG", (sdfnew["lonG"]+loni).cast(StringType()))
          sdf1 = sdf1.union(sdf1000000)

    sdf1 = sdf1.withColumn("tempkey", concat(sdf1["latG"],lit("|") ))
    sdf1 = sdf1.withColumn("key", concat(sdf1["tempkey"],sdf1["lonG"])).repartition(20,'key')

    sdf2 = sdf2.select(col("name").alias("hotelname"), col("lat").alias("hlat"), col("lon").alias("hlon"),col("key").alias("key"), col("Type").alias("Type"),col("key").alias("hkey")).repartition(9,'hkey')
    
    sdf1= sdf1.join(sdf2, sdf1.key == sdf2.hkey,how='inner')
  else:


    sdf2 = sdf2.withColumn("latG", (0*sdf2["lat"]).cast(IntegerType())).withColumn("lonG", (0*sdf2["lon"]).cast(IntegerType()))
    sdf2 = sdf2.withColumn("latG", sdf2["latG"].cast(StringType())).withColumn("lonG", sdf2["lonG"].cast(StringType()))

    sdf2 = sdf2.withColumn("tempkey", concat(sdf2["latG"],lit("|") ))
    sdf2 = sdf2.withColumn("key", concat(sdf2["tempkey"],sdf2["lonG"]) )

    sdf1= sdf1.withColumn("latG", (0*sdf1["lat"]).cast(IntegerType()))
    sdf1 = sdf1.withColumn("lonG", (0*sdf1["lon"]).cast(IntegerType()))
    sdf1 = sdf1.withColumn("tempkey", concat(sdf1["latG"],lit("|") ))
    sdf1 = sdf1.withColumn("key", concat(sdf1["tempkey"],sdf1["lonG"])).repartition(20,'key')

    sdf2 = sdf2.select(col("name").alias("hotelname"), col("lat").alias("hlat"), col("lon").alias("hlon"),col("key").alias("key"), col("Type").alias("Type"),col("key").alias("hkey")).repartition(9,'hkey')
    
    sdf1= sdf1.join(sdf2, sdf1.key == sdf2.hkey,how='inner')

  #elapsed time to merge points
  elapsed_time_merge_points = timeit.default_timer() - elapsed_time_merge_points

  #counting the points
  CountPoints=sdf1.count()

  #elapsed time to calculate the distance
  elapsed_time_calculate_distance = timeit.default_timer()

  #calculating distance
  sdf1 = sdf1.withColumn("Distance", udf_get_distance(sdf1.lon, sdf1.lat,(sdf1.hlon), sdf1.hlat).cast(DoubleType()))

  #elapsed time to calculate the distance
  elapsed_time_calculate_distance = timeit.default_timer() - elapsed_time_calculate_distance

  #elapsed time to filter
  elapsed_time_filter = timeit.default_timer()

  #filtering to return the results with the right distance
  sdf1 = sdf1.filter(sdf1.Distance <= distance)

  #elapsed time to filter
  elapsed_time_filter = timeit.default_timer() - elapsed_time_filter

  #counting the total distances
  CountFinalPoints=sdf1.count()

  #elapsed time to show
  elapsed_time_show = timeit.default_timer()

  sdf1.show()

  #elapsed time to show
  elapsed_time_show = timeit.default_timer() - elapsed_time_show


  flag='version1'
  print('elapsed_time_df:',elapsed_time_df)
  print('elapsed_time_findgrid:',elapsed_time_findgrid)
  print('elapsed_time_merge_points:',elapsed_time_merge_points)
  print('elapsed_time_calculate_distance:',elapsed_time_calculate_distance)
  print('elapsed_time_filter:',elapsed_time_filter)
  print('elapsed_time_show:',elapsed_time_show)
  return flag,elapsed_time_df,elapsed_time_findgrid,elapsed_time_merge_points,elapsed_time_calculate_distance,elapsed_time_filter,elapsed_time_show,multiplier,CountPoints,CountFinalPoints


# In[6]:



def pyspark_distance_calculator2(distance):
  #Timer
  elapsed_time_df = timeit.default_timer()
  

  #Loading the first Dataset
  sdf1 = spark.createDataFrame(df1)
  sdf1 = sdf1.withColumn("Type", lit("FirstOne"))

  #Loading the second Dataset
  sdf2 = spark.createDataFrame(df2)
  sdf2 = sdf2.withColumn("Type", lit("SecondOne"))

  #elapsed time to read dataframes
  elapsed_time_df = timeit.default_timer() - elapsed_time_df

  #elapsed time to read grid
  elapsed_time_findgrid = timeit.default_timer()

  #finding the point which is nearest to north or south
  place1=sdf1.groupBy().min('lat', 'lon')
  place2=sdf1.groupBy().max('lat', 'lon')
  place3=sdf2.groupBy().min('lat', 'lon')
  place4=sdf2.groupBy().max('lat', 'lon')
  appended = place1.union(place2)
  appended = appended.union(place3)
  appended = appended.union(place4)
  appended = appended.select(col("min(lat)").alias("minlat"), col("min(lon)").alias("minlot"))
  appended = appended.withColumn('Value',abs(appended.minlat).cast(DoubleType()))
  appended = appended.withColumn('minlot1',appended.minlot.cast(DoubleType()))
  appended= appended.groupBy().max('minlot1', 'Value')
  appended = appended.select(col("max(minlot1)").alias("lon"), col("max(Value)").alias("lat"))

  #Creating Points by decimal number in order to find the distance 
  havers10 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+10, appended.lat).cast(DoubleType())).withColumn("Delta", lit(0.1)).withColumn("Number",lit(1))
  findbestdistance = havers10
  for i in range(1,10):
      havers1 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+i, appended.lat).cast(DoubleType())).withColumn("Delta", lit(1)).withColumn("Number",lit(i))
      havers01 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+i*(0.1), appended.lat).cast(DoubleType())).withColumn("Delta", lit(10)).withColumn("Number",lit(i))
      havers001 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+i*0.01, appended.lat).cast(DoubleType())).withColumn("Delta", lit(100)).withColumn("Number",lit(i))
      havers0001 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+i*0.001, appended.lat).cast(DoubleType())).withColumn("Delta", lit(1000)).withColumn("Number",lit(i))
      havers00001 = appended.withColumn("ABS_DISTANCE", udf_get_distance(appended.lon, appended.lat,(appended.lon)+i*0.0001, appended.lat).cast(DoubleType())).withColumn("Delta", lit(10000)).withColumn("Number",lit(i))
      findbestdistance = findbestdistance.union(havers1)
      findbestdistance = findbestdistance.union(havers01)
      findbestdistance = findbestdistance.union(havers001)
      findbestdistance = findbestdistance.union(havers0001)
      findbestdistance = findbestdistance.union(havers00001)
      
  findbestdistance=findbestdistance.withColumn("Deltafromdistance",findbestdistance.ABS_DISTANCE-distance)
  mindelta = findbestdistance.filter(findbestdistance.Deltafromdistance>0)
  mindelta= mindelta.groupBy().min('Deltafromdistance')
  mindelta = mindelta.select(col("min(Deltafromdistance)").alias("minimun"))
  left_join = findbestdistance.join(mindelta, findbestdistance.Deltafromdistance == mindelta.minimun,how='inner')
  left_join = left_join.select('Delta','Number')

  #elapsed time to read grid
  elapsed_time_findgrid  = timeit.default_timer() - elapsed_time_findgrid
  
  #elapsed time to merge points
  elapsed_time_merge_points = timeit.default_timer()

  #find the nearest points and duplicate points
  try:
    multiplier= left_join.collect()[0][0]
    limit=left_join.collect()[0][1]
  except:
    multiplier= 150000
  if multiplier<=90000:
    sdf2=sdf2.withColumn("Delta",lit(multiplier))
    sdf1=sdf1.withColumn("Delta",lit(multiplier))
    sdf2 = sdf2.withColumn("latG", (sdf2["Delta"]*sdf2["lat"]).cast(IntegerType())).withColumn("lonG", (sdf2["Delta"]*sdf2["lon"]).cast(IntegerType()))
    sdf2 = sdf2.withColumn("latG", sdf2["latG"].cast(StringType())).withColumn("lonG", sdf2["lonG"].cast(StringType()))
    sdf2 = sdf2.withColumn("tempkey", concat(sdf2["latG"],lit("|") ))
    sdf2 = sdf2.withColumn("key", concat(sdf2["tempkey"],sdf2["lonG"]) )
    sdf1= sdf1.withColumn("latG", (sdf1["Delta"]*sdf1["lat"]).cast(IntegerType()))
    sdf1 = sdf1.withColumn("lonG", (sdf1["Delta"]*sdf1["lon"]).cast(IntegerType()))
    sdfnew = sdf1
    for lati in range(-1*limit,limit+1):
      for loni in range(-1*limit,limit+1):
        if lati !=0 or loni!=0  :
          sdf1000000 = sdfnew.withColumn("latG", (sdfnew["latG"]+lati).cast(StringType())).withColumn("lonG", (sdfnew["lonG"]+loni).cast(StringType()))
          sdf1 = sdf1.union(sdf1000000)
    sdf1 = sdf1.withColumn("tempkey", concat(sdf1["latG"],lit("|") ))
    sdf1 = sdf1.withColumn("key", concat(sdf1["tempkey"],sdf1["lonG"])).repartition(20,'key')
    sdf2 = sdf2.select(col("name").alias("hotelname"), col("lat").alias("hlat"), col("lon").alias("hlon"),col("key").alias("key"), col("Type").alias("Type"),col("key").alias("hkey")).repartition(9,'hkey')
    sdf1= sdf1.join(sdf2, sdf1.key == sdf2.hkey,how='inner')
  else:
    sdf2 = sdf2.withColumn("latG", (0*sdf2["lat"]).cast(IntegerType())).withColumn("lonG", (0*sdf2["lon"]).cast(IntegerType()))
    sdf2 = sdf2.withColumn("latG", sdf2["latG"].cast(StringType())).withColumn("lonG", sdf2["lonG"].cast(StringType()))
    sdf2 = sdf2.withColumn("tempkey", concat(sdf2["latG"],lit("|") ))
    sdf2 = sdf2.withColumn("key", concat(sdf2["tempkey"],sdf2["lonG"]) )
    sdf1= sdf1.withColumn("latG", (0*sdf1["lat"]).cast(IntegerType()))
    sdf1 = sdf1.withColumn("lonG", (0*sdf1["lon"]).cast(IntegerType()))
    sdf1 = sdf1.withColumn("tempkey", concat(sdf1["latG"],lit("|") ))
    sdf1 = sdf1.withColumn("key", concat(sdf1["tempkey"],sdf1["lonG"])).repartition(20,'key')
    sdf2 = sdf2.select(col("name").alias("hotelname"), col("lat").alias("hlat"), col("lon").alias("hlon"),col("key").alias("key"), col("Type").alias("Type"),col("key").alias("hkey")).repartition(9,'hkey')
    sdf1= sdf1.join(sdf2, sdf1.key == sdf2.hkey,how='inner')
  
  #elapsed time to merge points
  elapsed_time_merge_points = timeit.default_timer() - elapsed_time_merge_points

  #counting the points
  CountPoints=sdf1.count()

  #elapsed time to calculate the distance
  elapsed_time_calculate_distance = timeit.default_timer()

  #calculating distance
  sdf1 = sdf1.withColumn("Distance", udf_get_distance(sdf1.lon, sdf1.lat,(sdf1.hlon), sdf1.hlat).cast(DoubleType()))

  #elapsed time to calculate the distance
  elapsed_time_calculate_distance = timeit.default_timer() - elapsed_time_calculate_distance

  #elapsed time to filter
  elapsed_time_filter = timeit.default_timer()

  #filtering to return the results with the right distance
  sdf1 = sdf1.filter(sdf1.Distance <= distance)

  #elapsed time to filter
  elapsed_time_filter = timeit.default_timer() - elapsed_time_filter

  #counting the total distances
  CountFinalPoints=sdf1.count()

  #elapsed time to show
  elapsed_time_show = timeit.default_timer()

  sdf1.show()

  #elapsed time to show
  elapsed_time_show = timeit.default_timer() - elapsed_time_show


  flag='version2'
  print('elapsed_time_df:',elapsed_time_df)
  print('elapsed_time_findgrid:',elapsed_time_findgrid)
  print('elapsed_time_merge_points:',elapsed_time_merge_points)
  print('elapsed_time_calculate_distance:',elapsed_time_calculate_distance)
  print('elapsed_time_filter:',elapsed_time_filter)
  print('elapsed_time_show:',elapsed_time_show)
  return flag,elapsed_time_df,elapsed_time_findgrid,elapsed_time_merge_points,elapsed_time_calculate_distance,elapsed_time_filter,elapsed_time_show,multiplier,CountPoints,CountFinalPoints


# In[7]:


now = timeit.default_timer()

# The valid range of latitude in degrees is -90 and +90 for the southern and northern hemisphere respectively. Longitude is in the range -180 and +180
listlatlon=[(10,10),(35,70),(70,140)]
# Giving the Size of the Datasets
listsize=[100000]
# Giving the distance between points
distances=[0.01,0.1,1,5,10,50]

results=[]
for numsize in listsize:
  for distance in distances:
    for point in listlatlon:

      #tranform points to lat lon
      latnum = point[0]*1000000
      lonnum = point[1]*1000000

      #creating the dataframe
      df1=pd.DataFrame()

      #creating random points points between given lat lon
      df1['lat'] = np.random.randint(-1*latnum,latnum,size=(numsize))
      df1['lon'] = np.random.randint(-1*lonnum,lonnum,size=(numsize))

      #tranform points to lat lon
      df1['lat']=df1['lat']/1000000
      df1['lon']=df1['lon']/1000000

      #creating random name
      df1['Name']='a'

      #filtering the dataset
      df1=df1[['lat','lon','Name']]

      # ax1 = df1.plot.scatter(x='lat',y='lon',figsize=(25,25))

      #creating the dataframe
      df2=pd.DataFrame()

      #creating random points points between given lat lon
      df2['lat'] = np.random.randint(-1*latnum,latnum,size=(numsize))
      df2['lon'] = np.random.randint(-1*lonnum,lonnum,size=(numsize))

      #tranform points to lat lon
      df2['lat']=df2['lat']/1000000
      df2['lon']=df2['lon']/1000000
      
      #creating random name
      df2['Name']='a'
      #filtering the dataset
      df2=df2[['lat','lon','Name']]

      # ax2 = df2.plot.scatter(x='lat',y='lon',figsize=(25,25))
      x = pyspark_distance_calculator1(distance)
      y = pyspark_distance_calculator2(distance)
      results.append(numsize)
      results.append(distance)
      results.append(point)
      results.append(x)
      results.append(y)
      print(results)

with open("file.txt", "w") as output:
    output.write(str(results))
end=timeit.default_timer() - now
print('Total Seconds',end)


# In[8]:
results
dfout=pd.DataFrame(list(results))
dfout.to_csv('outputfinal1.csv')




