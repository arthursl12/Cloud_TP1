import pyspark
import os
import subprocess
import time
import datetime
import operator
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from subprocess import Popen, PIPE
from pyspark.sql import SparkSession
from collections import defaultdict
from pyspark.sql.window import Window

import pyspark.sql.functions as F
from pyspark.sql.functions import col, asc,desc
from pyspark.sql.functions import col, max as max_, min as min_, first, when
from pyspark.sql.functions import trunc, avg, countDistinct
from pyspark.sql.functions import dayofmonth,date_trunc,to_date,current_timestamp

# Create spark session
USER = 'arthurlima'  # Username to use in HDFS
spark = SparkSession.builder \
    .master("yarn")  \
    .appName("Activity3") \
    .getOrCreate()

# Generate full filenames
hdfs_prefix = 'hdfs://localhost:54310'; N=2
# hdfs_prefix = 'hdfs://localhost:9000'; N=31
prefix = f'{hdfs_prefix}/datasets/covid/'
filename_prefix = prefix+'part-'
filename_suffix = '-5f4af8d5-3171-48e9-9a56-c5a7c7a84cc3-c000.json'

# Read all into a dataframe
filenames = []
for i in range(N):
    filenames.append(filename_prefix+f"{i:05d}"+filename_suffix)
# print(filenames)
df = spark.read.json(filenames)
# Convert datetime string to datetime type
df = df.withColumn("created_at", col("created_at").cast("timestamp"))

# Find how many (distinct) users (not tweets) used the tag each day
tag = "#Trump"
evolution = (
    df 
        .select(date_trunc('day', df.created_at).alias('day'),"screen_name")
        .where(col("text").contains(tag))
        .where((df.country_code == "US") | (df.place_type == "US"))
        .groupBy("day")
        .agg(countDistinct("screen_name").alias("count"))
)

# Weekly rolling average window
w = (Window()
     .orderBy("day")
     .rowsBetween(-7, 0))

rolling = evolution.withColumn('rolling_average', avg('count').over(w))
rolling = rolling.select("day","rolling_average")

# Write CSV
rolling.coalesce(1).write.csv(hdfs_prefix+"/user/"+USER+"/task3")

# Convert last dataframe into dictionary for plotting
rows = [list(row) for row in rolling.collect()]
timeseries = {}
for [date, avg] in rows:
    timeseries[date] = avg

# Generate plot    
plt.figure(figsize=(30,10))
plt.title(f"Rolling Average - US Users tweeting '{tag}' in April 2020", fontsize=30)
plt.plot(timeseries.keys(),timeseries.values(),marker="o")
plt.ylabel("Rolling Average",fontsize=20)
plt.yticks(fontsize=20)
ax = plt.gca()
myFmt = mdates.DateFormatter('%m-%d')
ax.xaxis.set_major_formatter(myFmt)
plt.xlabel("Date",fontsize=20)
plt.xticks(fontsize=20, rotation=75)
plt.savefig('rolling_average.png', bbox_inches='tight')