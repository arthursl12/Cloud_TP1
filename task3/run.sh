set -e
USER="arthurlima"

# Remove output folder in HDFS
hdfs dfs -rm -r -f /user/$USER/task3
rm -rf *.csv

# Run 
spark-submit task3.py

# Get output 
hdfs dfs -ls /user/$USER/task3
hdfs dfs -get /user/$USER/task3/*

rm -rf _SUCCESS