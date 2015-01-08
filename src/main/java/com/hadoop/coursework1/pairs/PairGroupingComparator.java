package com.hadoop.coursework1.pairs;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.coursework1.Pair;

/**
 * 
 * @author pmonteiro
 *
 */
public class PairGroupingComparator extends WritableComparator {

	protected PairGroupingComparator() {
		super(Pair.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2) {
		Pair k1 = (Pair) w1;
		Pair k2 = (Pair) w2;

		return k1.getWordPair().compareTo(k2.getWordPair());
	}
}