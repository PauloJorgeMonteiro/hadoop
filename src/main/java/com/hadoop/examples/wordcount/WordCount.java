package com.hadoop.examples.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * This Hadoop program will count the most used words in the Martin Luther King
 * speech (http://www.americanrhetoric.com/speeches/mlkihaveadream.htm) <br>
 * 
 * To run from within Eclipse in a stand-alone mode, provide the following
 * arguments: <br>
 * assets/wordcount/input assets/wordcount/output <br>
 * 
 * @author pmonteiro
 *
 */
public class WordCount {

	public static void main(String[] args) throws Exception {

		String[] parameters = { "assets/mlk_speech/input", "assets/mlk_speech/output" };
		if (args != null && args.length == 2) {
			parameters = args;
		}

		Configuration conf = new Configuration();
		String[] folders = new GenericOptionsParser(conf, parameters).getRemainingArgs();
		Job job = new Job(conf, "WordCount");
		job.setJarByClass(WordCount.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(folders[0]));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(parameters[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(folders[1]));

		if (!job.waitForCompletion(true))
			return;
	}
}