package com.hadoop.coursework2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Useful for Hadoop solutions that involves Composite Keys <br>
 * When using Composite keys, the defaut partitioner, <b>HashPartitioner</b>, is
 * no longer able to insure that all records related to the original key go to
 * the same reducer (partition). <br>
 * In this scenario the partitioner needs to consider the original key part
 * while deciding on the partition for the record.<br>
 * 
 * @author pmonteiro
 * @since 07-01-2015
 *
 */
public class NodePartitioner extends Partitioner<Text, Node> {

	@Override
	public int getPartition(Text key, Node value, int numReduceTasks) {
		return key.toString().hashCode() % numReduceTasks;
	}

}
