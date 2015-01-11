package com.hadoop.coursework2.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import lombok.Data;

import org.apache.hadoop.io.WritableComparable;

public @Data class NodeWritable implements WritableComparable<NodeWritable> {

	public static final Integer PRECISION = 10;
	public static final Double INITIAL_PAGE_RANK = new Double(1.0);

	private String from;
	private String to;
	private Double previousPageRank = new Double(0.0);
	private int totalLinks;

	public NodeWritable() {
		super();
	}

	public NodeWritable(String from, String to) {
		this.from = from;
		this.to = to;
	}

	public NodeWritable(String from, String to, int totalLinks) {
		this.from = from;
		this.to = to;
		this.totalLinks = totalLinks;
	}

	public NodeWritable(String from, String to, int totalLinks, Double previousPageRank) {
		this.from = from;
		this.to = to;
		this.totalLinks = totalLinks;
		this.previousPageRank = previousPageRank;
	}

	public Double getPageRank() {
		Double pageRank = new Double(0.0);
		if (previousPageRank != 0) {
			pageRank = BigDecimal.valueOf(previousPageRank)
					.divide(BigDecimal.valueOf(totalLinks), PRECISION, RoundingMode.HALF_UP).doubleValue();
		} else {
			pageRank = BigDecimal.valueOf(INITIAL_PAGE_RANK)
					.divide(BigDecimal.valueOf(totalLinks), PRECISION, RoundingMode.HALF_UP).doubleValue();
		}
		return pageRank;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.from = in.readUTF();
		this.to = in.readUTF();
		this.totalLinks = in.readInt();
		this.previousPageRank = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(from);
		out.writeUTF(to);
		out.writeInt(totalLinks);
		out.writeDouble(previousPageRank.doubleValue());
	}

	@Override
	public int compareTo(NodeWritable node) {
		return this.from.compareTo(node.getFrom());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof NodeWritable))
			return false;
		NodeWritable other = (NodeWritable) obj;
		if (from == null) {
			if (other.from != null)
				return false;
		} else if (!from.equals(other.from))
			return false;
		if (totalLinks != other.totalLinks)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((from == null) ? 0 : from.hashCode());
		result = prime * result + totalLinks;
		return result;
	}

}
