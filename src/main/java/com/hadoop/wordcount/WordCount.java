package com.hadoop.wordcount;

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
 * To run from within Eclipse in a stand-alone mode, provide the following arguments: <br> 
 * assets/wordcount/input assets/wordcount/output <br>
 * 
 * @author pmonteiro
 *
 */
public class WordCount {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] folders = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = new  Job(conf, "WordCount");
        job.setJarByClass(WordCount.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(folders[0]));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true); 
		FileOutputFormat.setOutputPath(job, new Path(folders[1]));

		if (!job.waitForCompletion(true))
			return;
	}
}