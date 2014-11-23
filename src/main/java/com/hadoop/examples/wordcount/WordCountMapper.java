package com.hadoop.examples.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * An improved WordCount mapper example: </br>
 * * ignores standard punctuation marks </br>
 * * ignores case
 * 
 * @author pmonteiro
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	  
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line, " \"\t\n\r\f,.:;?![]`");
	    
	    while (itr.hasMoreTokens()) {
	       word.set(itr.nextToken().toLowerCase());
	       context.write(word, one);
	    }
	}
}