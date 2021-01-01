import os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col, lit, to_timestamp, year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import hour, count, dayofyear
from pyspark import SparkFiles
import pandas as pd
import requests

# create spark session
spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

# make spark run faster
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

# # raw data from nychealth
# url = 'https://s3.amazonaws.com/tripdata/202011-citibike-tripdata.csv.zip'
# # get webpage
# res = requests.get(url, allow_redirects=True)

# save page info to file
# with open('citibike_data.csv','wb') as file:
#     file.write(res.content)

# read csv and save to df

df = spark.read.csv("/citidata/202011-citibike-tripdata.csv",header=True)

# turn stoptime column to timestamp from string
df = df.withColumn("stoptime_ts",to_timestamp(col("stoptime")))

# turn stoptime column to timestamp from string
df = df.withColumn("starttime_ts",to_timestamp(col("starttime")))

# select columns to drop
columns_to_drop = ['tripduration',
 'starttime',
 'stoptime',
 'start station id',
 'start station latitude',
 'start station longitude',
 'end station id',
 'end station latitude',
 'end station longitude']

df = df.drop(*columns_to_drop)

df = df.withColumn("stoptime_date",date_format('stoptime_ts','MM/dd/yyy'))
df = df.withColumn('stoptime_time', date_format('stoptime_ts', 'HH:mm:ss'))


# create new dataframe grouping by time, location, and counting the number of bikes
final_df = (df.groupBy("stoptime_date", hour("stoptime_ts").alias("hour"), "start station name", "end station name").agg(count("bikeid").alias("no_of_trips")))

## Rename columns
final_df = final_df.withColumnRenamed("start station name", "start_station_name").withColumnRenamed("end station name", "end_station_name")

final_df.write.csv('transformed_citibike.csv')