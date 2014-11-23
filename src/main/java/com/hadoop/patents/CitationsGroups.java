package com.hadoop.patents;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * http://www.nber.org/patents/apat63_99.zip (description) <br> <br>
 * For each patent, we want to find and group the patents that cite it <br>
 * 
 * @author pmonteiro
 *
 */
public class CitationsGroups extends Configured implements Tool {

	public static class MapClass extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, key);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String csv = "";
			for (Text value : values) {
				if (csv.length() > 0)
					csv += ",";
				csv += value.toString();
			}
			context.write(key, new Text(csv));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "CitationsGroups");
		job.setJarByClass(CitationsGroups.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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

		String[] parameters = {"assets/patents/input","assets/patents/output"}; 
		if (args != null && args.length == 2) {
			parameters = args;
		}
		int res = ToolRunner.run(new Configuration(), new CitationsGroups(), parameters);
		System.exit(res);
	}
}