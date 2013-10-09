package Tasks;

import java.io.IOException;
import java.util.HashMap;
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

/**  Task 5/e [10 Points]
 * Determine which people have too much free time on their hand and if they have favorites or not.
That is, for each facebook page owner, determine how many total accesses to facebook pages
they have made (as reported in the AccessLog) as well as how many distinct facebook pages they
have accessed in total.

 * analyze: 1. count the number of same "ByWho" in access_log, 
 * 			2. count the number of distinct "WhatPage" for each "Bywho"
 *
 * @author Yanqing Zhou
 * date: Oct 6 2013
 * time: 14:14
 * 
 * input: access_log 
 * 			AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime
 * output: ByWho report
 *			"ByWho"	 "total number of viewed page" "total number of distinct viewed page"
 * mapper: <key: IntWritable "ByWho"; value: IntWritable "WhatPage">
 * 
 * unfinished yet, error
 */
public class task5YQ {

	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private IntWritable outputKey;
		private IntWritable outputValue;
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output, Reporter reportor)
						throws IOException {
			String line = value.toString();
			String[] tokens = line.split(",");//AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime
			int byWho = Integer.valueOf(tokens[1]);// 1 = ByWho
			int whatPage = Integer.valueOf(tokens[2]);// 2 = WhatPage
			outputKey = new IntWritable(byWho);
			outputValue = new IntWritable(whatPage);
			output.collect(outputKey, outputValue);
		}
	}
	
	public static class Reduce extends MapReduceBase implements 
	Reducer<IntWritable, IntWritable, IntWritable, Text> {

		@Override
		public void reduce(IntWritable keyByWho, Iterator<IntWritable> valuesWhatPage,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			int sumTotal = 0;
			int sumDistinct = 0;
			HashMap<Integer, Boolean> checked = new  HashMap<Integer, Boolean>();
			int temp;
			while (valuesWhatPage.hasNext()) {
				sumTotal ++;
				temp = valuesWhatPage.next().get();
				if(!checked.containsKey(temp)){ //disctinct
					checked.put(temp, true);
					sumDistinct++;
				}	
			}
			String result = "Totally viewed "+ sumTotal +" pages, and " + sumDistinct + " different pages";
			output.collect(keyByWho,new Text(result));
		}
		
	}
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(task5YQ.class);
		conf.setJobName("Task 5: total number of viewed page");
		
		
		
		conf.setMapperClass(Map.class);
		conf.setMapOutputKeyClass(IntWritable.class);//change mapper output class
		conf.setMapOutputValueClass(IntWritable.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		//conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}

}
