package com.hadoop.coursework2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.coursework2.model.NodeWritable;

/**
 * 
 * @author pmonteiro
 *
 */
public class NodeGroupingComparator extends WritableComparator {

	protected NodeGroupingComparator() {
		super(NodeWritable.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2) {
		NodeWritable k1 = (NodeWritable) w1;
		NodeWritable k2 = (NodeWritable) w2;

		return k1.getName().compareTo(k2.getName());
	}
}