package com.hadoop.coursework1.stripes;

import static com.hadoop.coursework1.stripes.StripeWritable.TOTAL;

import java.io.IOException;
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

import com.hadoop.coursework1.Pair;
import com.hadoop.coursework1.util.ParagrapghInputFormat;

/**
 * <b>Implementation of the Hadoop 'Stripes' design pattern</b><br>
 * <br>
 * Like the pairs approach, co-occurring word pairs are generated by two nested
 * loops. <br>
 * However, the major difference is that instead of emitting intermediate
 * key-value pairs for each co-occurring word pair, co-occurrence information is
 * first stored in an associative array, denoted H. <br>
 * <b>The mapper</b> emits key-value pairs with words as keys and corresponding
 * associative arrays as values, where each associative array encodes the
 * co-occurrence counts of the neighbors of a particular word (i.e., its
 * context) <br>
 * 
 * <i>(https://chandramanitiwary.wordpress.com/2012/08/19/map-reduce-design-
 * patterns-pairs-stripes/)</i> <br>
 * 
 * @author pmonteiro
 *
 */
public class StripesApproach extends Configured implements Tool {

	private static final String REDUCER_PARAM = "-r";
	private static final String KEYWORD = "KEYWORD";
	private static final String KEYWORD_DEFAULT_VALUE = "for";
	private static final String NEIGHBOURS = "neighbours";
	private static final String NEIGHBOURS_DEFAULT_VALUE = "1";

	public static class MapClass extends Mapper<LongWritable, Text, Text, StripeWritable> {

		private static Logger _log = Logger.getLogger(MapClass.class.getName());
		private Map<String, StripeWritable> stripes = new HashMap<String, StripeWritable>();

		public void map(LongWritable lineNumber, Text line, Context context) throws IOException, InterruptedException {

			int neighbours = context.getConfiguration().getInt(NEIGHBOURS, Integer.valueOf(NEIGHBOURS_DEFAULT_VALUE));
			String[] words = line.toString().toLowerCase().replaceAll("[^a-zA-Z0-9 \\-]", "").replaceAll("-", " ")
					.split("\\s+");

			for (int i = 0; i < words.length; i++) {

				if (words[i].trim().length() == 0) {
					continue;
				}

				if ((i + neighbours) >= words.length) {
					break;
				}

				if (stripes.containsKey(words[i])) {
					StripeWritable stripe = stripes.get(words[i]);
					stripes.put(words[i],
							stripe.add(words[i], words[i + neighbours], 1 + stripe.get(words[i + neighbours])));
				} else {
					stripes.put(words[i], new StripeWritable().add(words[i], words[i + neighbours], 1));
				}
			}

			if (stripes.size() > 0) {
				for (String term : stripes.keySet()) {
					log(context, term);
					context.write(new Text(term), stripes.get(term));
				}
				stripes.clear();
			}
		}

		private void log(Context context, String term) {
			if (context.getConfiguration().get(KEYWORD, KEYWORD_DEFAULT_VALUE).equals(term)) {
				_log.info("Emiting: " + term + " => " + stripes.get(term));
			} else {
				_log.debug("Emiting: " + term + " => " + stripes.get(term));
			}
		}
	}

	private static class Combine extends Reducer<Text, StripeWritable, Text, StripeWritable> {
		private static Logger _log = Logger.getLogger(Combine.class.getName());

		public void reduce(Text term, Iterable<StripeWritable> stripes, Context context) throws IOException,
				InterruptedException {

			StripeWritable stripeFreq = new StripeWritable();
			for (StripeWritable stripe : stripes) {
				sumKeyValues(term, stripe, stripeFreq);
			}
			log(context, term, stripeFreq);
			context.write(term, stripeFreq);
		}

		private void sumKeyValues(Text term, StripeWritable stripe, StripeWritable stripeFreq) {
			for (String word : stripe.getStripe().keySet()) {
				if (stripeFreq.containsKey(word)) {
					stripeFreq.add(term.toString(), word, stripeFreq.get(word) + stripe.get(word));
				} else {
					stripeFreq.add(term.toString(), word, stripe.get(word));
				}
			}
		}

		private void log(Context context, Text term, StripeWritable stripeFreq) {
			if (context.getConfiguration().get(KEYWORD, KEYWORD_DEFAULT_VALUE).equals(term.toString())) {
				_log.info("Emiting: " + term.toString() + " => " + stripeFreq);
			} else {
				_log.debug("Emiting: " + term.toString() + " => " + stripeFreq);
			}
		}
	}

	public static class Reduce extends Reducer<Text, StripeWritable, Text, DoubleWritable> {
		private static Logger _log = Logger.getLogger(Reduce.class.getName());

		public void reduce(Text term, Iterable<StripeWritable> stripes, Context context) throws IOException,
				InterruptedException {

			StripeWritable stripeFreq = new StripeWritable();
			for (StripeWritable stripe : stripes) {
				sumKeyValues(term, stripe, stripeFreq);
			}
			emitFrequencies(term, stripeFreq, context);
		}

		private void sumKeyValues(Text term, StripeWritable stripe, StripeWritable stripeFreq) {
			for (String word : stripe.getStripe().keySet()) {
				if (stripeFreq.containsKey(word.toString())) {
					stripeFreq.add(term.toString(), word, stripeFreq.get(word) + stripe.get(word));
				} else {
					stripeFreq.add(term.toString(), word, stripe.get(word));
				}
				stripeFreq.add(
						term.toString(),
						TOTAL,
						((stripeFreq.get(TOTAL) == null || stripeFreq.get(TOTAL) == 0) ? stripe.get(word) : (stripeFreq
								.get(TOTAL) + stripe.get(word))));
			}
		}

		private void emitFrequencies(Text term, StripeWritable stripe, Context context) throws IOException,
				InterruptedException {
			Text key = new Text();
			DoubleWritable value = new DoubleWritable();
			stripe.calculateFrequencies();
			while (!stripe.getQueue().isEmpty()) {
				Pair pair = stripe.getQueue().poll();
				key.set(pair.toString());
				value.set(pair.getConditionalProbability());
				log(context, pair);
				context.write(key, value);
			}
			stripe.clear();
		}

		private void log(Context context, Pair pair) {
			if (context.getConfiguration().get(KEYWORD, KEYWORD_DEFAULT_VALUE).equals(pair.getTerm())) {
				_log.info("Emiting: " + pair.toString());
			} else {
				_log.debug("Emiting: " + pair.toString());
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Coursework 1 - Stripes Approach");
		job.setInputFormatClass(ParagrapghInputFormat.class);
		job.setJarByClass(StripesApproach.class);

		job.getConfiguration().set(KEYWORD, KEYWORD_DEFAULT_VALUE);
		job.getConfiguration().set(NEIGHBOURS, NEIGHBOURS_DEFAULT_VALUE);

		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StripeWritable.class);

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
		// String[] parameters = { "assets/mlk_speech/input/I_have_a_dream2.txt", "assets/mlk_speech/output" };
		// String[] parameters = { "assets/mlk_speech/input", "assets/mlk_speech/output" };
		String[] parameters = { "assets/jane_austen/input", "assets/jane_austen/output"};
		if (args != null && (args.length == 2 || args.length == 3)) {
			parameters = args;
		}
		int res = ToolRunner.run(new Configuration(), new StripesApproach(), parameters);
		System.exit(res);
	}
}