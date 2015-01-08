package com.hadoop.coursework1.pairs;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.coursework1.Pair;

/**
 * 
 * To guarantee that all of them composite keys will come in the same input
 * group (i.e. in a single reduce() call) as the list of values. <br>
 * TO ensure that we compare on the original key. <br>
 * 
 * @author pmonteiro
 *
 */
public class PairSortComparator extends WritableComparator {

	protected PairSortComparator() {
		super(Pair.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2) {
		Pair p1 = (Pair) w1;
		Pair p2 = (Pair) w2;

		int result = p1.getWordPair().compareTo(p2.getWordPair());
		return result;
	}
}