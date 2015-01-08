package com.hadoop.coursework1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;


/**
 * 
 * @author pmonteiro
 *
 */
@AllArgsConstructor
public @Data class Pair implements Comparable<Pair>, WritableComparable<Pair> {

	public static final String TOTAL = "*";
	public static final String WHITESPACE = " ";
	
	private String term;
	private Integer termCount = 0;
	private String word;
	private Integer wordCount = 0;

	public Pair() {
		super();
	}
	
	public String getWordPair() {
		return new StringBuilder().append(term).append(WHITESPACE).append(word).toString();
	}
	
	public String getConditionalProbabilityStr() {
		return wordCount + "/" + termCount;
	}

	public Double getConditionalProbability() {
		return new BigDecimal(wordCount).divide(new BigDecimal(termCount), 5, RoundingMode.HALF_UP).doubleValue();
	}

	@Override
	public int compareTo(Pair obj) {
		if (obj == null || StringUtils.isBlank(obj.getTerm())) {
			return -1;
		}

		if (!this.term.equals(obj.term)) {
			return -1;
		}

		if (this.termCount <= 0 || obj.getTermCount() <= 0 || (getConditionalProbability().compareTo(obj.getConditionalProbability()) == 0) ) {
			if (wordCount.compareTo(obj.getWordCount()) == 0) {
				return getWordPair().compareTo(obj.getWordPair());
			}
			return wordCount.compareTo(obj.getWordCount());
		}
		return -1*getConditionalProbability().compareTo(obj.getConditionalProbability());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.term = in.readUTF();
		this.termCount = in.readInt();
		this.word = in.readUTF();
		this.wordCount = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(term);
		out.writeInt(termCount);
		out.writeUTF(word);
		out.writeInt(wordCount);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (term == null) {
			if (other.term != null)
				return false;
		} else if (!term.equals(other.term))
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((term == null) ? 0 : term.hashCode());
		result = prime * result + ((word == null) ? 0 : word.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return getWordPair() + " (" + getConditionalProbabilityStr() + ")";
	}
}
