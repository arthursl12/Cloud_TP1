set -e
USER="arthurlima"

# Remove output folder in HDFS
hdfs dfs -rm -r -f /user/$USER/task2
rm -rf *.csv

# Run 
spark-submit task2.py

# Get output 
hdfs dfs -ls /user/$USER/task2
hdfs dfs -get /user/$USER/task2/*

rm -rf _SUCCESS