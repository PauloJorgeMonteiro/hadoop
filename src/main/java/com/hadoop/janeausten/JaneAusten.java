package com.hadoop.janeausten;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.wordcount.WordCount;

/**
 * 
 * @author pmonteiro
 *
 */
public class JaneAusten extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line =  value.toString();
			String [] words = line.toLowerCase().replaceAll("[^a-zA-Z ]", "").split("\\s+");
			
			for (int i = 1; i < words.length; i++) {
				context.write(new Text(words[i-1] + " " + words[i]), one);
			}
			
//			StringTokenizer itr = new StringTokenizer(value.toString(), " \"\t\n\r\f,.:;?![]`");
		    
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] folders = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = new  Job(conf, "Pairs");
        job.setJarByClass(WordCount.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(folders[0]));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true); 
		FileOutputFormat.setOutputPath(job, new Path(folders[1]));

		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		private int totalWords = 0;
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				  sum += val.get();
			}
			
			if (sum > 5) {
				result.set(sum);
				context.write(key, result);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		String[] parameters = {"assets/jane_austen/input","assets/jane_austen/output"}; 
		int res = ToolRunner.run(new Configuration(), new JaneAusten(), parameters);
		System.exit(res);
	}
}