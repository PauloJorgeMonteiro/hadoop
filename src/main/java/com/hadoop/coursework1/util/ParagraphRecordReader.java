package com.hadoop.coursework1.util;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

public class ParagraphRecordReader extends RecordReader<LongWritable, Text> {
	
	private static Logger _log = Logger.getLogger(ParagraphRecordReader.class.getName());
	
	private static final String WHITESPACE = " ";
	
	private RecordReader<LongWritable, Text> lineRecord = new LineRecordReader();
    private LongWritable key = new LongWritable();
    private Text value = new Text();

	public ParagraphRecordReader(InputSplit split, TaskAttemptContext context) {
		try {
			initialize(split, context);
		} catch (IOException e) {
			_log.error("Error while initializing " + this.getClass().getSimpleName(), e);
		} catch (InterruptedException e) {
			_log.error("Error while initializing " + this.getClass().getSimpleName(), e);
		}
	}

	@Override
	public void close() throws IOException {
		lineRecord.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecord.getProgress();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		lineRecord.initialize(split,context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
        while (lineRecord.nextKeyValue()) {
        	sb.append(lineRecord.getCurrentValue());
        	sb.append(WHITESPACE);
        	if (StringUtils.isBlank(lineRecord.getCurrentValue().toString())) {
        		key = lineRecord.getCurrentKey();
        		value.set(sb.toString());
        		sb.append(WHITESPACE);
        		return true;
        	}
        }
        return false;
	}
}
