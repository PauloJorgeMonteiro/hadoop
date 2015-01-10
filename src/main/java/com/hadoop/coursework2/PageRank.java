package com.hadoop.coursework2;

import static com.hadoop.coursework2.util.MultiLineRecordReader.SEMICOLON;
import static com.hadoop.coursework2.util.MultiLineRecordReader.TAB;
import static com.hadoop.coursework2.util.MultiLineRecordReader.WITH_VALUE;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hadoop.coursework2.model.NodeWritable;
import com.hadoop.coursework2.util.MultiLineInputFormat;

/**
 * Cloud Computing Cousework 2 - Implementation of PageRank<br>
 * <br>
 * 
 * @author pmonteiro
 *
 */
public class PageRank extends Configured implements Tool {


	public static class MapClass extends Mapper<LongWritable, Text, Text, NodeWritable> {

		private static Logger _log = Logger.getLogger(MapClass.class.getName());
		Text key = new Text();

		public void map(LongWritable lineNumber, Text multiLine, Context context) throws IOException,
				InterruptedException {
			String[] lines = multiLine.toString().split(SEMICOLON);
			for (String line : lines) {
				String[] nodes = line.toString().split(TAB);
				NodeWritable fromNode = new NodeWritable(nodes[0], nodes[1], lines.length);
				key.set(fromNode.getTo());
				_log.debug("Emiting: " + key + WITH_VALUE + fromNode);
				context.write(key, fromNode);
			}
		}
	}

	public static class Reduce extends Reducer<Text, NodeWritable, Text, DoubleWritable> {
		private final static BigDecimal DAMPING_FACTOR = new BigDecimal(0.85);
		
		private static Logger _log = Logger.getLogger(Reduce.class.getName());
		private Map<Text, List<NodeWritable>> nodesMap = new HashMap<>();
		private DoubleWritable value = new DoubleWritable(0);
		private Integer totalNodes = 0;

		public void reduce(Text key, Iterable<NodeWritable> values, Context context) throws IOException,
				InterruptedException {

			totalNodes++;
			
			List<NodeWritable> nodes = new ArrayList<>();
			for (NodeWritable node : values) {
				nodes.add(new NodeWritable(node.getFrom(), node.getTo(), node.getTotalLinks(), node.getPreviousPageRank()));
			}
			nodesMap.put(new Text(key), nodes);

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Text key : nodesMap.keySet()) {
				value.set(calculateCompletePageRank(nodesMap.get(key)));
				_log.debug("Emiting: " + key + " => " + value);
				context.write(key, value);
			}
		}

		/**
		 * Calculates the simplified <b>PageRank</b> for a given node
		 * 
		 * @param nodes
		 * @return PageRank
		 */
		private double calculateSimplePageRank(List<NodeWritable> nodes) {
			double totalPageRank = 0.0;
			for (NodeWritable node : nodes) {
				totalPageRank += node.getPageRank();
			}
			return BigDecimal.valueOf(totalPageRank).divide(BigDecimal.valueOf(totalNodes), 5, RoundingMode.HALF_UP).doubleValue();
		}
		
		/**
		 * Calculates the complete <b>PageRank</b> for a given node. <br>
		 * This method makes use of the damping factor.<br>
		 * The equations is: <i>(1-d)/N + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))</i>. <br>
		 * As seen in Wikipedia: http://en.wikipedia.org/wiki/PageRank#Damping_factor
		 * 
		 * @param nodes
		 * @return PageRank
		 */
		public double calculateCompletePageRank(List<NodeWritable> nodes) {
			BigDecimal firstPart = BigDecimal.valueOf(1).min(DAMPING_FACTOR).divide(BigDecimal.valueOf(totalNodes), 5, RoundingMode.HALF_UP);
			
			double linksPageRank = 0.0;
			for (NodeWritable node : nodes) {
				linksPageRank += node.getPageRank();
			}
			BigDecimal secondPart = DAMPING_FACTOR.multiply(BigDecimal.valueOf(linksPageRank));
			
			return firstPart.add(secondPart).divide(BigDecimal.valueOf(totalNodes), 5, RoundingMode.HALF_UP).doubleValue();
		}

	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Coursework 2 - PageRank");
		job.setInputFormatClass(MultiLineInputFormat.class);

		job.setJarByClass(PageRank.class);

		job.setMapperClass(MapClass.class);
//		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NodeWritable.class);
		// job.setPartitionerClass(NodePartitioner.class);
		// job.setSortComparatorClass(NodeSortComparator.class);
		// job.setGroupingComparatorClass(NodeGroupingComparator.class);

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
		String[] parameters = { "assets/pagerank/input/pagerank02.txt", "assets/pagerank/output" };
		if (args != null && args.length == 2) {
			parameters = args;
		}
		int res = ToolRunner.run(new Configuration(), new PageRank(), parameters);
		System.exit(res);
	}
}