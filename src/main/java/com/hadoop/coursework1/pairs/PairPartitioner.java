package com.hadoop.coursework1.pairs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.hadoop.coursework1.Pair;

/**
 * Useful for Hadoop solutions that involves Composite Keys <br>
 * When using Composite keys, the defaut partitioner, <b>HashPartitioner</b>, is
 * no longer able to insure that all records related to the original key go to
 * the same reducer (partition). <br>
 * In this scenario the partitioner needs to consider the original key part
 * while deciding on the partition for the record.<br>
 * 
 * @author pmonteiro
 * @since 29-11-2014
 *
 */
public class PairPartitioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair key, IntWritable value, int numReduceTasks) {
		return key.getTerm().hashCode() % numReduceTasks;
	}

}
