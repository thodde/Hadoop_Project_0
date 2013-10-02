/**
 * @author Trevor Hodde
 * @class Task D
 * @description For each facebook page compute the 'happiness' factor
 * of its owner. That is, for each facebook page in your dataset, report the owners
 * name, and the number of number of people listing them as a friend.
 *
 * Found the idea for prepending values to strings for simple parsing here:
 * http://stackoverflow.com/questions/6351224/parse-text-file-line-by-line-skipping-certain-lines
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class Task4 {
    /**
     * This mapper class collects all the pageID numbers and the names of the corresponding users
     */
    public static class PageMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text pageID = new Text();  // Stores the ID of the current users page
        private String name;               // Stores the name of the current user
        private String fileTag ="PAGE,";   // This prepends the word PAGE to particular lines to help us later parse things out
    
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] splitLine = line.split(",");
            pageID.set(splitLine[0]);
            name = splitLine[1];
            output.collect(pageID, new Text(fileTag + name));
        }
    }
  
    /**
     * This Mapper class collects all the page IDs of friends from the friend data set
     */
    public static class FriendMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text pageID = new Text();   // Stores the ID of the current page
        private String fileTag ="FRIEND,";  // Prepends the word FRIEND to lines so we can parse things easier later
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] splits = line.split(",");
            pageID.set(splits[2]);
            output.collect(pageID, new Text(fileTag));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private String name;              // The name of the current user
        private long totalFriendCount;    // The total number of friends the current user has

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)throws IOException{
            totalFriendCount = 0;

            while (values.hasNext()) {
                String line = values.next().toString();
                String splits[] = line.split(",");

                // If the first value in the string is PAGE, we know it is just the pageID of the user
                if(splits[0].equals("PAGE")) {
                    name = splits[1];
                }
                else if(splits[0].equals("FRIEND")) {   // Otherwise, we found a friend!
                    totalFriendCount += 1;
                }
            }

            Text text = new Text();
            text.set(name + "," + Long.toString(totalFriendCount));
            output.collect(text, new Text()); 
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Task4.class);
        conf.setJobName("Task 4: happiness factor");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
  
        MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, PageMapper.class);
        MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, FriendMapper.class);
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        JobClient.runJob(conf);
    }
}
