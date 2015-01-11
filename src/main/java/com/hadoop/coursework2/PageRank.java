package com.hadoop.coursework2;

import static com.hadoop.coursework2.util.MultiLineRecordReader.SEMICOLON;
import static com.hadoop.coursework2.util.MultiLineRecordReader.TAB;
import static com.hadoop.coursework2.util.MultiLineRecordReader.WITH_VALUE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

import com.hadoop.coursework2.model.Node;
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

	private static final String REDUCER_PARAM = "-r";
	
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

		private static Logger _log = Logger.getLogger(Reduce.class.getName());
		private Map<Text, Node> nodesMap = new HashMap<>();
		private List<Node> nodesMap2 = new ArrayList<>();
		private Text key = new Text();
		private DoubleWritable value = new DoubleWritable(0);
		private Queue<Node> priorityQueue = new PriorityQueue<>();
		private Integer totalNodes = 0;

		public void reduce(Text key, Iterable<NodeWritable> values, Context context) throws IOException,
				InterruptedException {

			totalNodes++;

			List<NodeWritable> nodes = new ArrayList<>();
			for (NodeWritable node : values) {
				nodes.add(new NodeWritable(node.getFrom(), node.getTo(), node.getTotalLinks(), node
						.getPreviousPageRank()));
			}
//			nodesMap.put(new Text(key), new Node(key.toString(), nodes));
			nodesMap2.add(new Node(key.toString(), nodes));

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			for (Node node : nodesMap2) {
				node.calculateCompletePageRank(totalNodes);
				_log.debug("Converging: " + node);
			}
			
//			convergePageRank();
			
			emitPageRank(context);
//			for (Text key : nodesMap.keySet()) {
////				value.set(nodesMap.get(key).calculateCompletePageRank(totalNodes));
//				value.set(nodesMap.get(key).calculateSimplePageRank(totalNodes));
//				_log.debug("Emiting: " + key + " => " + value);
//				context.write(key, value);
//			}
		}

		private void convergePageRank() {
			boolean hasConverged = true;
			do {
				hasConverged = true;
				for (Node node : nodesMap2) {
					for (Node node2 : nodesMap2) {
						if (!node.equals(node2)) {
							for(NodeWritable nw : node2.getNodes()) {
								if (node.getName().equals(nw.getFrom())) {
									nw.setPreviousPageRank(node.getRank());
									node.setPreviousRank(node.getRank());
								}
							}
						}
					}
				}
				
				for (Node node : nodesMap2) {
					node.calculateCompletePageRank(totalNodes);
					 if ( !node.isConverged() ) {
						 hasConverged = false;
					 }
					_log.debug("Converging: " + node);
				}
			} while (!hasConverged);
		}

		private void emitPageRank(Context context) throws IOException, InterruptedException {
			_log.debug("Creating priority queue...");
			priorityQueue.addAll(nodesMap2);
			while (!priorityQueue.isEmpty()) {
				Node node = priorityQueue.poll();
				key.set(node.getName());
				value.set(node.getRank());
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
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NodeWritable.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if (REDUCER_PARAM.equals(args[i])) {
					job.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " + other_args.size() + " instead of 2.");
			return printUsage();
		}
		
		
		Path in = new Path(args[0]);
		FileInputFormat.setInputPaths(job, in);

		Path out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	private static int printUsage() {
		System.out.println("stripesApproach [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public static void main(String[] args) throws Exception {
//		 String[] parameters = { "assets/pagerank/input/pagerank02.txt", "assets/pagerank/output" };
//		String[] parameters = { "assets/pagerank/input/pagerank03.txt", "assets/pagerank/output" };
//		String[] parameters = { "assets/pagerank/input/pagerank04.txt", "assets/pagerank/output" };
//		String[] parameters = { "assets/pagerank/input/pagerank05.txt", "assets/pagerank/output" };
		 String[] parameters = { "assets/epinions_social_network/input", "assets/epinions_social_network/output" };
		 if (args != null && (args.length == 2 || args.length == 3)) {
			parameters = args;
		}
		int res = ToolRunner.run(new Configuration(), new PageRank(), parameters);
		System.exit(res);
	}
}