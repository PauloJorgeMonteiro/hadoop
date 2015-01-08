package com.hadoop.coursework1.stripes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import lombok.Getter;
import lombok.Setter;

import org.apache.hadoop.io.WritableComparable;

import com.hadoop.coursework1.Pair;

/**
 * Wrapper class to the MapWritable. Overriding the toString() method in order
 * to print into the file.
 * 
 * @author pmonteiro
 *
 */

public class StripeWritable implements WritableComparable<StripeWritable> {

	public static final String TOTAL = "*";
	
	private @Getter @Setter String term;
	private @Getter @Setter Map<String, Integer> stripe = new HashMap<>();
	private @Getter @Setter Queue<Pair> queue = new PriorityQueue<>();


	public StripeWritable add(String term, String word, Integer count) {
		this.term =term;
		stripe.put(word, count);
		return this;
	}
	
	public Integer get(String word) {
		Integer value = (Integer) stripe.get(word);
		if (value == null) {
			return 0;
		} else {
			return value;
		}
	}

	public boolean containsKey(String word) {
		return this.getStripe().containsKey(word);
	}
	
	public StripeWritable calculateFrequencies() {
		for (String word : stripe.keySet()) {
			if (word != null && !TOTAL.equals(word)) {
				Pair pair = new Pair(term, stripe.get(TOTAL), word, stripe.get(word));
				queue.add(pair);
			}
		}		
		return this;
	}

	public void clear() {
		stripe.clear();
		queue.clear();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		term = in.readUTF();

		int stripesSize = in.readInt();
		stripe.clear();
		for (int i = 0; i < stripesSize; i++) {
			stripe.put(in.readUTF(), in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.term);

		out.writeInt(stripe.size());
		for (String word : stripe.keySet()) {
			out.writeUTF(word);
			out.writeInt(stripe.get(word));
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (String word : stripe.keySet()) {
			sb.append(word.toString() + ":" + stripe.get(word));
			if (stripe.size() > 0) {
				sb.append(" ");
			}
		}
		return sb.toString();
	}

	@Override
	public int compareTo(StripeWritable stripe) {
        return this.getTerm().compareTo(stripe.getTerm());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((term == null) ? 0 : term.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof StripeWritable))
			return false;
		StripeWritable other = (StripeWritable) obj;
		if (term == null) {
			if (other.term != null)
				return false;
		}
		if (other.term == null) 
			return false;
		if (term.equalsIgnoreCase(other.getTerm())) 
			return true;
		return false;
	}

}
