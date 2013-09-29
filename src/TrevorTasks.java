/**
 * User: trevor hodde
 * Date: 9/29/13
 * Time: 2:44 PM
 */
import java.util.*;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TrevorTasks {
    public static class Map extends Mapper<Text, Text, Text, Text> {
        private Text word = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(key, word);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
         private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String str = "";

            for (Text val : values) {
                str += "|" + val.toString();
            }

            result.set(str);
            context.write(key, result);
        }
    }

    public void doTaskB(File inputFile) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "socialNetwork");
        job.setJarByClass(TrevorTasks.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputFile.toString()));
        FileOutputFormat.setOutputPath(job, new Path("."));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
