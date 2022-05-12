set -e

USER="arthurlima"

# Compile and Run
export HADOOP_CP=$(hadoop classpath)
mkdir -p classes
javac --release 8 -cp "$HADOOP_CP:./json-simple-1.1.1.jar:." -d classes TextArrayWritable.java IntTextArrayWritable.java Hashtags.java 
jar -cvf Hashtags.jar -C classes/ .
hdfs dfs -rm -r -f /user/$USER/actv1
hadoop jar Hashtags.jar hashtags.Hashtags /datasets/covid /user/$USER/actv1

# Get output, select 500 most popular tags 
hdfs dfs -ls /user/$USER/actv1
hdfs dfs -cat /user/$USER/actv1/result/part-r-00000
hdfs dfs -get /user/$USER/actv1/result/part-r-00000 /home/$USER/activity1/part-r-00000.csv
sort -k2 -r -n -t, part-r-00000.csv > ordered_part-r-00000.csv
head -n 500 ordered_part-r-00000.csv > ordered_500_part-r-00000.csv