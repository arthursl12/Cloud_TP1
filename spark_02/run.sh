set -e

# Remove output folder in HDFS
hdfs dfs -rm -r -f /user/arthurlima/task3
rm -rf task3

# Run 
spark-submit activity3.py

# Get output 
hdfs dfs -ls /user/arthurlima/task3
hdfs dfs -get /user/arthurlima/task3/*

# Add csv extension to it
filename=$(find part*)
mv $filename $filename.csv