set -e

# Compile and Run
export HADOOP_CP=$(hadoop classpath)
javac --release 8 -cp "$HADOOP_CP:./json-simple-1.1.1.jar:." -d classes TextArrayWritable.java IntTextArrayWritable.java Hashtags.java 
jar -cvf Hashtags.jar -C classes/ .
hdfs dfs -rm -r -f /user/arthurlima/actv1/out01
hadoop jar Hashtags.jar hashtags.Hashtags /datasets/covid /user/arthurlima/actv1/out01

# Get output, select 500 most popular tags 
hdfs dfs -ls /user/arthurlima/actv1/out01
hdfs dfs -cat /user/arthurlima/actv1/out01/result/part-r-00000
hdfs dfs -get /user/arthurlima/actv1/out01/result/part-r-00000 /home/arthurlima/activity1/part-r-00000.csv
sort -k2 -r -n -t, part-r-00000.csv > ordered_part-r-00000.csv
head -n 500 ordered_part-r-00000.csv > ordered_500_part-r-00000.csv