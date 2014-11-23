package com.hadoop.patents;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * To Run this example please download the following files: <br>
 * http://www.nber.org/patents/acite75_99.zip (citations) <br>
 * http://www.nber.org/patents/apat63_99.zip (description) <br>
 * <br>
 * For the patent citation data, we may want the number of citations a patent
 * has received.<br>
 * (<i>Lam, Chuck. Hadoop in Action. Greenwich, CT: Manning Publications,
 * 2011)</i> <br>
 * 
 * @author pmonteiro
 *
 */
public class CitationsCount extends Configured implements Tool {

	public static class MapClass extends Mapper<Text, Text, IntWritable, IntWritable> {
		private IntWritable keyVal = new IntWritable();
		private IntWritable one = new IntWritable(1);

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if (StringUtils.isNumeric(value.toString())) {
				keyVal.set(Integer.parseInt(value.toString()));
				context.write(keyVal, one);
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable counter = new IntWritable(0);

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {

			int count = 0;
			for (IntWritable val : values) {
				  count += val.get();
			}
			if (count > 100) {
				counter.set(count);
				context.write(key, counter);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "CitationsCount");
		job.setJarByClass(CitationsCount.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
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

		String[] parameters = { "assets/patents/input", "assets/patents/output" };
		if (args != null && args.length == 2) {
			parameters = args;
		}
		int res = ToolRunner.run(new Configuration(), new CitationsCount(), parameters);
		System.exit(res);
	}
}