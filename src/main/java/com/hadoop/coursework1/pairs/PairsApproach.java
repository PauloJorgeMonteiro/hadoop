package com.hadoop.coursework1.pairs;

import static com.hadoop.coursework1.Pair.TOTAL;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.log4j.Logger;

import com.hadoop.coursework1.Pair;
import com.hadoop.coursework1.util.ParagrapghInputFormat;

/**
 * Cloud Computing Cousework 1 - Implementation of the Hadoop Pairs design
 * pattern<br>
 * <br>
 * 
 * @author pmonteiro
 *
 */
public class PairsApproach extends Configured implements Tool {

	private static final String KEYWORD = "KEYWORD";
	private static final String NEIGHBOURS = "neighbours";
	private static final int NEIGHBOURS_DEFAULT_VALUE = 1;

	public static class MapClass extends Mapper<LongWritable, Text, Pair, IntWritable> {

		private static Logger _log = Logger.getLogger(MapClass.class.getName());
		private Map<String, Pair> pairs = new HashMap<>();
		private IntWritable value = new IntWritable(1);

		public void map(LongWritable lineNumber, Text line, Context context) throws IOException, InterruptedException {
			int neighbours = context.getConfiguration().getInt(NEIGHBOURS, NEIGHBOURS_DEFAULT_VALUE);
			String[] words = line.toString().toLowerCase()
					.replaceAll("[^a-zA-Z0-9 \\-]", "").replaceAll("-", " ").split("\\s+");

			for (int i = 0; i < words.length; i++) {

				if (words[i].trim().length() == 0) {
					continue;
				}

				if ((i + neighbours) >= words.length) {
					break;
				}

				String keyStrPair = words[i] + " " + words[i + neighbours];
				String relativeStrPair = words[i] + " " + TOTAL;
				if (pairs.containsKey(keyStrPair)) {
					pairs.get(keyStrPair).setWordCount(pairs.get(keyStrPair).getWordCount()+1);
					pairs.get(relativeStrPair).setWordCount(pairs.get(relativeStrPair).getWordCount()+1);
				} else {
					Pair cKey = new Pair();
					cKey.setTerm(words[i]);
					cKey.setWord(words[i + neighbours]);
					cKey.setWordCount(1);
					pairs.put(cKey.getWordPair(), cKey);
					if (pairs.containsKey(relativeStrPair)) {
						pairs.get(relativeStrPair).setWordCount(pairs.get(relativeStrPair).getWordCount()+1);
					} else {
						Pair relativePair = new Pair();
						relativePair.setTerm(words[i]);
						relativePair.setWord(TOTAL);
						relativePair.setWordCount(1);
						pairs.put(relativePair.getWordPair(), relativePair);
					}
				}
			}
			
			for (String key : pairs.keySet()) {
				value.set(pairs.get(key).getWordCount());
				_log.debug("Emiting: " + pairs.get(key));
				context.write(pairs.get(key), value);
			}
			pairs.clear();
		}
	}

	public static class Combiner extends Reducer<Pair, IntWritable, Pair, IntWritable> {
		private static Logger _log = Logger.getLogger(Combiner.class.getName());
		private IntWritable counter = new IntWritable(0);

		public void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int counts = 0;
			for (IntWritable val : values) {
				counts += val.get();
			}

			key.setWordCount(counts);
			counter.set(counts);
			_log.debug("Emiting: " + key);
			context.write(key, counter);
		}
	}

	public static class Reduce extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
		private static Logger _log = Logger.getLogger(Reduce.class.getName());
		private Queue<Pair> queue = new PriorityQueue<>();
		private Integer termCount = 0;
		private String term;
		private DoubleWritable value = new DoubleWritable(0);

		public void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {

			if(StringUtils.isNotBlank(term) && !term.equals(key.getTerm())) {
				emit(context);
			}
			
			int wordCount = 0;
			for (IntWritable val : values) {
				wordCount += val.get();
			}
			
			if (key.getWordPair().matches(".*\\*")) {
				term = key.getTerm();
				termCount = wordCount;
			} else {
				Pair p = new Pair();
				p.setTerm(key.getTerm());
				p.setTermCount(termCount);
				p.setWord(key.getWord());
				p.setWordCount(wordCount);
				queue.add(p);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			 emit(context);
        }

		private void emit(Context context) throws IOException, InterruptedException {
			while (!queue.isEmpty()) {
				Pair pair = queue.poll();
				value.set(pair.getConditionalProbability());
				_log.debug("Emiting: " + pair.toString());
				context.write(pair, value);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Coursework 1 - Pairs Approach");
		job.setInputFormatClass(ParagrapghInputFormat.class);
		
		if (args.length == 4) {
			job.getConfiguration().set(KEYWORD, args[2]);
			job.getConfiguration().set(NEIGHBOURS, args[3]);
		}
		job.setJarByClass(PairsApproach.class);

		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(PairPartitioner.class);
		job.setSortComparatorClass(PairSortComparator.class);
		job.setGroupingComparatorClass(PairGroupingComparator.class);

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
//		String[] parameters = { "assets/mlk_speech/input/I_have_a_dream2.txt", "assets/mlk_speech/output", "for", "1" };
//		String[] parameters = { "assets/mlk_speech/input", "assets/mlk_speech/output", "for", "1" };
		 String[] parameters = { "assets/jane_austen/input", "assets/jane_austen/output", "for", "1" };
		if (args != null && (args.length > 2 && args.length < 5)) {
			parameters = args;
		}
		int res = ToolRunner.run(new Configuration(), new PairsApproach(), parameters);
		System.exit(res);
	}
}