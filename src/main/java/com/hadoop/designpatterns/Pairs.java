package com.hadoop.designpatterns;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Implementation of the Hadoop Pairs design pattern<br>
 * <br>
 * <b>The mapper</b> processes each input document and emits intermediate
 * key-value pairs with each co-occurring word pair as the key and the integer
 * one. <br>
 * The neighbors of a word can either be defined in terms of a sliding window or
 * some other contextual unit such as a sentence.<br>
 * 
 * <i>(https://chandramanitiwary.wordpress.com/2012/08/19/map-reduce-design-
 * patterns-pairs-stripes/)</i> <br>
 * 
 * @author pmonteiro
 *
 */
public class Pairs extends Configured implements Tool {

	private static final String NEIGHBOURS = "neighbours";
	private static final int NEIGHBOURS_DEFAULT_VALUE = 1;

	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final Text pair = new Text();
		private final IntWritable one = new IntWritable(1);

		public void map(LongWritable lineNumber, Text line, Context context) throws IOException, InterruptedException {

			int neighbours = context.getConfiguration().getInt(NEIGHBOURS, NEIGHBOURS_DEFAULT_VALUE);
			String[] words = line.toString().toLowerCase().replaceAll("[^a-zA-Z ]", "").split("\\s+");

			for (int i = 0; i < words.length; i++) {

				if (words[i].length() == 0) {
					continue;
				}
				
				for (int j = i - neighbours; j < i + neighbours + 1; j++) {

					if (j >= words.length) {
						break;
					}

					if (j == i || j < 0) {
						continue;
					}

					pair.set(words[i] + " " + words[j]);
					context.write(pair, one);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable counter = new IntWritable(0);

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			
			counter.set(count);
			context.write(key, counter);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "CitationsGroups");
		if (args.length == 3) {
			job.getConfiguration().set(NEIGHBOURS, args[2]);
		}
		job.setJarByClass(Pairs.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path in = new Path(args[0]);
		FileInputFormat.setInputPaths(job, in);

		Path out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		String[] parameters = { "assets/mlk_speech/input", "assets/mlk_speech/output", "2" };
		if (args != null && (args.length == 2 || args.length == 3)) {
			parameters = args;
		}
		int res = ToolRunner.run(new Configuration(), new Pairs(), parameters);
		System.exit(res);
	}
}