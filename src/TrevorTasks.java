/**
 * User: trevor hodde
 * Date: 9/29/13
 * Time: 2:44 PM
 */
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrevorTasks {
    public static class Map extends Mapper<IntWritable, Text, Text, IntWritable, Text> {
        private Text word = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(key, word);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, IntWritable, Text> {
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

    public void doTaskB() {
        JobConf conf = new JobConf(TrevorTasks.class);
        conf.setJobName("socialNetwork");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);

        //boolean result = job.waitForCompletion(true);
        //System.exit(result ? 0 : 1);
    }
}
