from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("yarn") \
    .appName("HelloLines") \
    .getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile("hdfs:/user/arthurlima/hello.txt")
lines = rdd.count()
outrdd = sc.parallelize([lines])
# The following will fail if the output directory exists:
outrdd.saveAsTextFile("hdfs:/user/arthurlima/hello-linecount-submit")
sc.stop()