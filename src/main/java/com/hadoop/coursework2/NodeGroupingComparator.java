package com.hadoop.coursework2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author pmonteiro
 *
 */
public class NodeGroupingComparator extends WritableComparator {

	protected NodeGroupingComparator() {
		super(Node.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2) {
		Node k1 = (Node) w1;
		Node k2 = (Node) w2;

		return k1.getName().compareTo(k2.getName());
	}
}