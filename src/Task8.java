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

/**
 * a) Task h [10 Points] This job returns all users that share a specified hobby (in our case Programming, but no mutual friends.
 * 
 * @author Trevor Hodde
 * date: Oct 10 2013
 * 
 * input: my_page 
 * 			ID, Name, Nationality, CountryCode, Hobby
 * output: Users that share a hobby but no friends
 * 	 
 */
public class Task8 {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reportor)
				throws IOException {
			String line = value.toString();
			String[] tokens = line.split(",");
			String result;

			// finds all users with the same hobby
			if (tokens[4].equals("Programming")) {
				// name (1) and hobby (4)
				result = tokens[1];
				word.set(result);
				output.collect(one, word);
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(Task8.class);
		conf.setJobName("Task 8: shared hobbies");

		conf.setMapperClass(Map.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// Need a reducer to remove users that are mutual friends

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

}
