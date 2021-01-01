import os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col, lit, to_timestamp, year, month, dayofmonth, hour, weekofyear, date_format
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

df = spark.read.csv(df = spark.read.csv("citidata/202011-citibike-tripdata.csv",header=True))
df.printSchema()

