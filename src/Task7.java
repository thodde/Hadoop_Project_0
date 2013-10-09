/**
 * @author Trevor Hodde
 * @class Task G
 * @description Find the list of people who have set up a facebook page and lost interest
 * after some amount of time.
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Task7 {
    /**
     * This mapper class collects all the user ID numbers
     */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text userID = new Text();
        
        public void map (LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] splits = line.split(",");
            userID.set(splits[1]);
            output.collect(userID, new Text(splits[4]));                 
        }
    }
  
    /**
     * This reducer class finds all access times that are greater than some initial value (we chose 850,000 to hopefully
     * make our output very small. This means any user whose lastAccessTime > 850,000 (the time of "page creation")
     * has lost interest and will be collected and added to our output.
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce (Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int timeSinceCreation = 850000;    // This number is fairly large (compared to 1,000,000) so the list should be rather small
            int lastAccessTime;

            while (values.hasNext()) {
                lastAccessTime = Integer.parseInt(values.next().toString());
                if (lastAccessTime <= timeSinceCreation)
                    continue;
                else
                    return;
            }
            output.collect(key, new Text()); 
        }
    }

      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(Task7.class);
          conf.setJobName("Task7");
  
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
  
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
  
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
  
          FileInputFormat.setInputPaths(conf, new Path(args[0]));
          FileOutputFormat.setOutputPath(conf, new Path(args[1]));
  
          JobClient.runJob(conf);
      }
  }
