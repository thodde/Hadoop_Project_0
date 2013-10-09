package Tasks;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/*
 * User: Yanqing Zhou
 * Date: 10/5/2013
 * Time: 5:08 PM
 */

/**
 * a) Task a [10 Points] Write a job(s) that reports all Facebook users (name,
 * and hobby) whose Nationality is the same as your own Nationality (pick one:
 * be it Chinese or German or whatever).
 * 
 * @author Yanqing Zhou
 * date: Oct 5 2013
 * time: 22:52
 * 
 * input: my_page 
 * 			ID, Name, Nationality, CountryCode, Hobby
 * output: Chinese Facebook users (name and hobby)
 * 	 
 */
public class task1YQ {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reportor)
				throws IOException {
			String line = value.toString();
			String[] tokens = line.split(",");// assume "," is functional only
			String result;
			if (tokens[3].equals("2")) {// 2=chinese
				// name (1) and hob (4)
				result = tokens[1] + " " + tokens[4];
				word.set(result);
				output.collect(one, word);
			}
		}
		// no need for reducer
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(task1YQ.class);
		conf.setJobName("Task 1: name and hobby of every Chinese user");

		conf.setMapperClass(Map.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		//job.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

}
