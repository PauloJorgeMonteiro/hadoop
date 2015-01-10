package com.hadoop.coursework2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.coursework2.model.NodeWritable;

/**
 * 
 * To guarantee that all of them composite keys will come in the same input
 * group (i.e. in a single reduce() call) as the list of values. <br>
 * TO ensure that we compare on the original key. <br>
 * 
 * @author pmonteiro
 *
 */
public class NodeSortComparator extends WritableComparator {

	protected NodeSortComparator() {
		super(NodeWritable.class, true);
	}

//	@Override
//	@SuppressWarnings("rawtypes")
//	public int compare(WritableComparable w1, WritableComparable w2) {
//		Node p1 = (Node) w1;
//		Node p2 = (Node) w2;
//
//		return p1.getName().compareTo(p2.getName());
//	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text p1 = (Text) w1;
		Text p2 = (Text) w2;

		return p1.toString().compareTo(p2.toString());
	}
}