set -e

USER="arthurlima"

# Preparing for results and compilation
mkdir -p classes
rm -rf *.csv

# Compile and Run
export HADOOP_CP=$(hadoop classpath)
javac --release 8 -cp "$HADOOP_CP:./json-simple-1.1.1.jar:." -d classes TextArrayWritable.java IntTextArrayWritable.java Hashtags.java 
jar -cvf Hashtags.jar -C classes/ .
hdfs dfs -rm -r -f /user/$USER/actv1
hadoop jar Hashtags.jar hashtags.Hashtags /datasets/covid /user/$USER/actv1

# Get output, select 500 most popular tags 
hdfs dfs -ls /user/$USER/actv1
# hdfs dfs -cat /user/$USER/actv1/result/part-r-00000
hdfs dfs -get /user/$USER/actv1/result/part-r-00000

# Add csv extension to it
filename=$(find part*)
mv $filename $filename.csv

# Sort and select first 500
sort -k2 -r -n -t, part-r-00000.csv > ordered_part-r-00000.csv
head -n 500 ordered_part-r-00000.csv > ordered_500_part-r-00000.csv

# Generate plots
jupyter nbconvert --to notebook --inplace --execute task1_plots.ipynb