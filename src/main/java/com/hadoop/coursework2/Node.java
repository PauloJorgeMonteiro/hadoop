package com.hadoop.coursework2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import org.apache.hadoop.io.WritableComparable;

public @Data class Node implements WritableComparable<Node> {

	private String name;
	private BigDecimal rank = new BigDecimal(0.0);
	private List<Node> fromEdges = new ArrayList<Node>();

	public Node() {
		super();
	}

	public Node(String name) {
		this.name = name;
	}

	public Node(String name, BigDecimal rank) {
		this.name = name;
		this.rank = rank;
	}

	public Node(String name, BigDecimal rank, List<Node> fromEdges) {
		this.name = name;
		this.rank = rank;
		this.fromEdges = fromEdges;
	}

//	public BigDecimal getRank() {
//		for (Node node : fromEdges) {
//			rank.add(node.getRank());
//		}
//		return rank;
//	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.rank = BigDecimal.valueOf(in.readDouble());
//		int fromEdgesSize = in.readInt();
//		fromEdges.clear();
//		for (int i = 0; i < fromEdgesSize; i++) {
//			fromEdges.add(new Node(in.readUTF(), BigDecimal.valueOf(in.readDouble()), null));
//		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeDouble(rank.doubleValue());
//		out.writeInt(fromEdges.size());
//		for (Node node : fromEdges) {
//			out.writeUTF(node.getName());
//			out.writeDouble(node.getRank().doubleValue());
//		}
	}

	@Override
	public int compareTo(Node arg0) {
		return name.compareTo(arg0.getName());
	}

}
