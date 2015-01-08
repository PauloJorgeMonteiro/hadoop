package com.hadoop.coursework2;

import static com.hadoop.coursework2.util.MultiLineRecordReader.SEMICOLON;
import static com.hadoop.coursework2.util.MultiLineRecordReader.TAB;
import static com.hadoop.coursework2.util.MultiLineRecordReader.WITH_VALUE;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

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

import com.hadoop.coursework1.Pair;
import com.hadoop.coursework2.util.MultiLineInputFormat;

/**
 * Cloud Computing Cousework 2 - Implementation of PageRank<br>
 * <br>
 * 
 * @author pmonteiro
 *
 */
public class PageRank extends Configured implements Tool {

	private BigDecimal dampingFactor = new BigDecimal(0.85);

	public static class MapClass extends Mapper<LongWritable, Text, Text, Node> {

		private static Logger _log = Logger.getLogger(MapClass.class.getName());
		Text key = new Text();

		public void map(LongWritable lineNumber, Text multiLine, Context context) throws IOException,
				InterruptedException {
			String[] lines = multiLine.toString().split(SEMICOLON);
			for (String line : lines) {
				String[] nodes = line.toString().split(TAB);
				Node node = new Node(nodes[1]);
				key.set(node.getName());
				Node fromNode = new Node(nodes[0], new BigDecimal(1).divide(BigDecimal.valueOf(lines.length), 5,
						RoundingMode.HALF_UP));
				node.getFromEdges().add(fromNode);
				_log.debug("Emiting: " + key + WITH_VALUE + fromNode);
				context.write(key, fromNode);
			}
		}
	}

	public static class Combiner extends Reducer<Text, Node, Text, Node> {
		private static Logger _log = Logger.getLogger(Combiner.class.getName());

		public void reduce(Text key, Iterable<Node> nodes, Context context) throws IOException, InterruptedException {

			Node totalNodes = new Node(key.toString());
			StringBuilder sb = new StringBuilder();
			for (Node n : nodes) {
				totalNodes.setRank(totalNodes.getRank().add(n.getRank()));
				totalNodes.getFromEdges().add(n);
			}

			_log.debug("Emiting: " + key + WITH_VALUE + totalNodes);
			context.write(key, totalNodes);
		}
	}

	public static class Reduce extends Reducer<Text, Node, Text, DoubleWritable> {
		private static Logger _log = Logger.getLogger(Reduce.class.getName());
		private Queue<Pair> queue = new PriorityQueue<>();
		private Map<Text, Node> nodesMap = new HashMap<>();
		private DoubleWritable value = new DoubleWritable(0);
		private Integer totalNodes = 0;

		public void reduce(Text key, Iterable<Node> values, Context context) throws IOException,
				InterruptedException {
			
			// TODO: Still need to sum all the nodes that have not been captured by the Combiner.
			//       This might need a new Writable structure. 
			
			totalNodes++;
			
			for (Node node : values) {
				Text text = new Text(key.toString());
				Node value = new Node(key.toString(), node.getRank(), node.getFromEdges());
				nodesMap.put(text, value);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Text key : nodesMap.keySet()) {
				value.set(nodesMap.get(key).getRank().divide(BigDecimal.valueOf(totalNodes), 5, RoundingMode.HALF_UP).doubleValue());
				_log.debug("Emiting: " + key + " => " + value);
				context.write(key, value);
			}
		}

	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Coursework 2 - PageRank");
		job.setInputFormatClass(MultiLineInputFormat.class);

		job.setJarByClass(PageRank.class);

		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
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