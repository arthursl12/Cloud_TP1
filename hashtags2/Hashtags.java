package hashtags;

// Standard Libs
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap; 
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.regex.Pattern;


// Hadoop and MapReduce
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

// JSON Simple
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class Hashtags {
    static String temp_filename = "temp.txt";
    static int POPULAR_MIN = 1000;

    // 1st Job Mapper: similar to WordCount
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            // Parse JSON tweet, get its text
            String tweet;
            String screen_name;
            try{
                JSONParser parser = new JSONParser();
                JSONObject obj = (JSONObject) parser.parse(value.toString());
                tweet = (String) obj.get("text");
                screen_name = (String) obj.get("screen_name");
            }catch (ParseException e){
                tweet = "";
                screen_name = "";
            }
            
            StringTokenizer itr = new StringTokenizer(tweet);
            while (itr.hasMoreTokens()) {
                // Iterate through each word in the line.
                String word = itr.nextToken();
                if(word.charAt(0) == '#'){
                    // This is a hashtag

                    // Filter out last character punctuation
                    while(word.length() > 1 && isPunctuaction(word.charAt(word.length()-1))){
                        // System.out.printf("Filtering #: %s\n", word);
                        word = removeLastChar(word);
                    }
                    // System.out.printf("Filtered #: %s\n", word);
                    if (word.length() > 1){
                        // Remove leading '#' symbol
                        String tag_without_symbol = word.substring(1);
                        context.write(new Text(tag_without_symbol), one);
                    } 
                }   
            }
        }
        private String removeLastChar(String s){  
            //returns the string after removing the last character  
            return s.substring(0, s.length() - 1);  
        }
        private boolean isPunctuaction(char c){
            //returns if the char is a punctuation (except underline)
            String c_str = Character.toString(c);
            if (c == '_'){
                return false;
            }else if (Pattern.matches("[\\p{Punct}\\p{IsPunctuation}]", c_str)){
                return true;
            }else{
                return false;
            }
        }      
    }

    // 1st Job Reducer: similar to WordCount, but only outputs tags with more
    // than 1000 ocurrences
    public static class IntSumReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key_hashtag, Iterable<IntWritable> values, Context ctx)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();   // For popularity metric
        }

        // Only output if popular hashtag
        if (sum >= POPULAR_MIN){
            result.set(sum);
            ctx.write(key_hashtag, result);
        }
        
    }
}


    // 2nd Job Mapper
    public static class TokenizerMapperFilter
        extends Mapper<Object, Text, Text, TextArrayWritable> {
        private TextArrayWritable hashtagsW = new TextArrayWritable();
        private HashMap<String,Integer> popular_tags = new HashMap<String,Integer>();
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get all popular tags from the previous MapReduce run
            Configuration conf = context.getConfiguration();
            String popular_tags_filename = conf.get("intermediate.result.location");
            Path path = new Path(popular_tags_filename);
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream fdsis = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fdsis));
            String line = "";
            ArrayList<String> lines = new ArrayList<String>();
            while ((line = br.readLine()) != null) {
                String[] pair = line.split(",");
                // System.out.printf("parseInt %s\n", line);
                String tag = pair[0];
                int popularity = Integer.parseInt(pair[1]); 
                popular_tags.put(tag,popularity);
            }
            br.close();
        }

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            // Parse JSON tweet, get its text
            String tweet;
            String screen_name;
            try{
                JSONParser parser = new JSONParser();
                JSONObject obj = (JSONObject) parser.parse(value.toString());
                tweet = (String) obj.get("text");
                screen_name = (String) obj.get("screen_name");
            }catch (ParseException e){
                tweet = "";
                screen_name = "";
            }

            // Find all hashtags, add only the popular ones to an array
            ArrayList<String> all_hashtags = new ArrayList<String>();
            StringTokenizer itr = new StringTokenizer(tweet);
            while (itr.hasMoreTokens()) {
                // Iterate through each word in the line.
                String word = itr.nextToken();
                if(word.charAt(0) == '#'){
                    // This is a hashtag
                    word = word.substring(1);
                    if (popular_tags.containsKey(word)){
                        // This is a popular hashtag
                        all_hashtags.add(word);
                    }
                }   
            }

            // Send to Reducers: pair (hashtag, list_of_related)
            Iterator<String> it = all_hashtags.iterator();
            String[] hashtagsArray = new String[all_hashtags.size()];
            hashtagsArray = all_hashtags.toArray(hashtagsArray);
            hashtagsW = new TextArrayWritable(hashtagsArray);
            while(it.hasNext()){
                String curr_hashtag = it.next();
                Text hashtag_text = new Text(curr_hashtag);
                context.write(hashtag_text, hashtagsW);
            }
        }

    }

    // 2nd Job Reducer
    public static class SumAndCorrelatedReducer
            extends Reducer<Text, TextArrayWritable, Text, Writable> {
        private Writable writable_result;
        private HashMap<String,Integer> popular_tags = new HashMap<String,Integer>();
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get all popular tags from the previous MapReduce run
            Configuration conf = context.getConfiguration();
            String popular_tags_filename = conf.get("intermediate.result.location");
            Path path = new Path(popular_tags_filename);
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream fdsis = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fdsis));
            String line = "";
            ArrayList<String> lines = new ArrayList<String>();
            while ((line = br.readLine()) != null) {
                String[] pair = line.split(",");
                String tag = pair[0];
                int popularity = Integer.parseInt(pair[1]); 
                popular_tags.put(tag,popularity);
            }
            br.close();
        }
        public void reduce(Text key_hashtag, Iterable<TextArrayWritable> all_related_hashtags, Context ctx)
                throws IOException, InterruptedException {
            Set<String> all_hashtags = new HashSet<String>();
            for (TextArrayWritable array : all_related_hashtags) {
                String[] some_hashtags = array.toStrings();
                for (String s : some_hashtags){
                    if (s != key_hashtag.toString()){
                        all_hashtags.add(s);
                    }
                }
            }
            
            // Convert set to array
            String[] hashtagsArray = new String[all_hashtags.size()];
            hashtagsArray = all_hashtags.toArray(hashtagsArray);
            writable_result = new IntTextArrayWritable(popular_tags.get(key_hashtag.toString()), hashtagsArray);
            ctx.write(key_hashtag, writable_result);
        }
    }

    public static void main(String[] args) throws Exception {
        // 2nd Job: hashtag count
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job1 = Job.getInstance(conf, "Actv1: count popular tags");
        job1.setJarByClass(Hashtags.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/tmp"));
        job1.waitForCompletion(true);

        // 2nd Job: count related
        conf = new Configuration();
        JobConf jobconf = new JobConf();
        
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("intermediate.result.location", (new URI(args[1]+"/tmp/part-r-00000").toString()));
        Job job2 = Job.getInstance(conf, "Actv1: count related hashtags");
        job2.addCacheArchive(new URI(args[1]+"/tmp/part-r-00000"));
        job2.setJarByClass(Hashtags.class);
        job2.setMapperClass(TokenizerMapperFilter.class);
        job2.setReducerClass(SumAndCorrelatedReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(TextArrayWritable.class);
        job2.setOutputValueClass(Writable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/result"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}


