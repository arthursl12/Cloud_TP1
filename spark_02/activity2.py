import pyspark
import os
import subprocess
import time
import datetime
import operator

from pyspark.sql import SparkSession
from collections import defaultdict
from pyspark.sql.functions import col, asc,desc
import pyspark.sql.functions as F
from pyspark.sql.functions import col, max as max_, min as min_, first, when

spark = SparkSession.builder.appName("Activity2").getOrCreate()

# Get all filenames
path = 'datasets/covid'
# cmd = 'ls ' + path
cmd = 'hdfs dfs -ls ' + path

files = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
filenames = []
for f in files:
    filenames.append(path + "/" + f)

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

result.coalesce(1).write.csv("hdfs:/user/arthurlima/actv2")

spark.sparkContext.stop()