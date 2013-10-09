package Tasks;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 *  Task 2/b [10 Points]
Write a job(s) that reports for each country, 
how many of its citizens have a Facebook page.

 * analyze: count the number of tuple for same "CountryCode" in my_page.csv, 
 * final private String[] Nationality = {"German", "French","Chinese","American","Indian","Italian","English","Japanese"};
 * @author Yanqing Zhou
 * date: Oct 6 2013
 * time: 17:12
 * 
 * input: my_page.csv 
 * 			ID Name Nationality CountryCode Hobby

 * output: every nation and its number of people
 *
 */
public class task2YQ {
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text countryCode = new Text();		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reportor)throws IOException {
			String line = value.toString();
			//ID,Name,Nationality,CountryCode,Hobby    3 = CountryCode
			countryCode.set(line.split(",")[3]);
			output.collect(countryCode, one);
		}
	}
	
	public static class Reduce extends MapReduceBase implements 
			Reducer<Text, IntWritable, Text, IntWritable > {
		private final String[] Nationality = {"German", "French","Chinese","American","Indian","Italian","English","Japanese"};
		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			int index = Integer.valueOf(key.toString());
			String nationality = Nationality[index];
			key.set(nationality);
			output.collect(key, new IntWritable(sum));
		}
	}
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(task2YQ.class);
		conf.setJobName("Task 2: National user count");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}

}
