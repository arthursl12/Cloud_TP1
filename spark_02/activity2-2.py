import pyspark
import os
import subprocess
import time
import datetime
import operator

from subprocess import Popen, PIPE
from pyspark.sql import SparkSession
from collections import defaultdict
from pyspark.sql.functions import col, asc,desc
import pyspark.sql.functions as F
from pyspark.sql.functions import col, max as max_, min as min_, first, when

print("IMPORTS")
spark = SparkSession.builder \
    .master("yarn")  \
    .appName("Activity2") \
    .getOrCreate()
print("SPARKSESSION")

# Get list of files from hdfs
# hdfs_prefix = 'hdfs://localhost:54310'
hdfs_prefix = 'hdfs://localhost:9000'
prefix = f'{hdfs_prefix}/datasets/covid/'
filename_prefix = prefix+'part-'
filename_suffix = '-5f4af8d5-3171-48e9-9a56-c5a7c7a84cc3-c000.json'

filenames = []
for i in range(31):
    filenames.append(filename_prefix+f"{i:05d}"+filename_suffix)
print("READ")
# Read all into a dataframe
df = spark.read.json(filenames)
# Convert datetime string to datetime type
df = df.withColumn("created_at", col("created_at").cast("timestamp"))

# Find the 1000 most active users
actives = (
    df \
        .groupby("screen_name") \
        .count() \
        .orderBy(col("count").desc()) \
        .limit(1000) \
)
print("ACTIVES")

# How many followers at the start of dataset, for verified accounts
fol_start = \
(
    df
        .filter(col("Verified") == 'TRUE')
        .withColumn("AB", F.struct("created_at", "followers_count"))
        .groupby("screen_name")
        # F.max(AB) selects AB-combinations with max `A`. If more
        # than one combination remains the one with max `B` is selected. If
        # after this identical combinations remain, a single one of them is picked
        # randomly.
        .agg(F.min("AB").alias("max_AB"))
        .select("screen_name", "max_AB.created_at", F.expr("max_AB.followers_count"))
        
)

# How many followers at the end of dataset, for verified accounts
fol_end = \
(
    df
        .filter(col("Verified") == 'TRUE')
        .withColumn("AB", F.struct("created_at", "followers_count"))
        .groupby("screen_name")
        # F.max(AB) selects AB-combinations with max `A`. If more
        # than one combination remains the one with max `B` is selected. If
        # after this identical combinations remain, a single one of them is picked
        # randomly.
        .agg(F.max("AB").alias("max_AB"))
        .select("screen_name", "max_AB.created_at", F.expr("max_AB.followers_count"))
        
)

# Join the two
joined = fol_end \
    .select("screen_name",fol_end.followers_count.alias("end_count")) \
    .join(fol_start, on="screen_name")

# How many followers gained the (top 1000 verified users)
delta_df  = (
    joined \
        .withColumn('Result', ( joined["end_count"] - joined["followers_count"] ) ) \
        .select("screen_name","Result") \
        .sort(desc("Result")) \
        .limit(1000)
        .withColumn("Result", col("Result").cast("int"))
)

result = (
delta_df \
    .join(actives, on="screen_name", how="left") \
    .select("screen_name", col("Result").alias("Foll. Gained"), when(col("count").isNotNull(), 1).otherwise(0).alias("IsActive")) \
)
print("RESULT")


result.coalesce(1).write.csv(hdfs_prefix+"/user/arthurlima/actv2/run002")
print("PRINT")

spark.sparkContext.stop()