package com.hadoop.coursework2.util;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

public class MultiLineRecordReader extends RecordReader<LongWritable, Text> {

	private static Logger _log = Logger.getLogger(MultiLineRecordReader.class.getName());

	public static final String SEMICOLON = ";";
	public static final String TAB = "\\t";
	public static final String WITH_VALUE = " => ";

	private RecordReader<LongWritable, Text> lineRecord = new LineRecordReader();
	private LongWritable key = new LongWritable();
	private Text value = new Text();

	private String previousLine;
	private String currentLine;
	private boolean sameIteration;

	public MultiLineRecordReader(InputSplit split, TaskAttemptContext context) {
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
		lineRecord.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		while (lineRecord.nextKeyValue()) {

			currentLine = lineRecord.getCurrentValue().toString();
			if (currentLine.startsWith("#")) {
				continue;
			}

			String[] nodes = currentLine.toString().split(TAB);
			if (previousLine == null) {
				// first time reading the file
				sb.append(currentLine);
				assignValue(sb);
				sameIteration = true;
			} else {
				String[] previousNodes = previousLine.toString().split(TAB);
				if (!nodes[0].equals(previousNodes[0])) {
					if (!sameIteration)
						sb.append(previousLine);

					assignValue(sb);
					sameIteration = false;
					return true;
				} else {
					if (!sameIteration)
						sb.append(previousLine);

					if (sb.length() > 0)
						sb.append(SEMICOLON);

					sb.append(currentLine);

					assignValue(sb);
					sameIteration = true;
				} 
			}
		}
		if (sameIteration) {
			// making sure we process the last value in the RecordReader
			sameIteration=false;
			return true;
		}
		return false;
	}

	private void assignValue(StringBuffer sb) throws IOException, InterruptedException {
		value.set(sb.toString());
		key = lineRecord.getCurrentKey();
		previousLine = currentLine;
	}
}
