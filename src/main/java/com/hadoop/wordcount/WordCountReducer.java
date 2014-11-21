package com.hadoop.wordcount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * An improved WordCount mapper example: </br>
 * * Only count words with more than 5 repetitions
 *  
 * @author pmonteiro
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable result = new IntWritable();
	
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