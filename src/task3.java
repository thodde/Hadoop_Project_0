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
 *  Task 3/c [10 Points]
Find the top 10 interesting Facebook pages, namely, 
those that got the most accesses based on your 
AccessLog dataset compared to all other pages.

 * analyze: count the number of same "WhatPage" in access_log, 
 * 			find the top 10, and out put pageID
 *
 * @author Yanqing Zhou
 * date: Oct 5 2013
 * time: 22:52
 * 
 * input: access_log 
 * 			AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime
 * output: 10 most frequent "WhatPage"
 *
 * unfinished yet
 */
public class task3YQ {
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reportor)
						throws IOException {
			String line = value.toString();
			String[] tokens = line.split(",");// assume "," is functional only
			String result = tokens[2];// 2 = WhatPage
			word.set(result);
			output.collect(word, one);
			
		}
	}
	
	public static class Reduce extends MapReduceBase implements 
	Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
		
	}
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(task3YQ.class);
		conf.setJobName("Task 3: top 10 pop page");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}

}
