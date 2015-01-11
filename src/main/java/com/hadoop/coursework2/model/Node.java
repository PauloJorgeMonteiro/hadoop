package com.hadoop.coursework2.model;

import static com.hadoop.coursework2.model.NodeWritable.PRECISION;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;

public @Data class Node implements Comparable<Node> {

	private static final Double DAMPING_FACTOR = new Double(0.7);
	 
	private String name;
	private double rank;
	private double previousRank;
	private boolean converged;
	private List<NodeWritable> nodes = new ArrayList<NodeWritable>();

	public Node() {
		super();
	}

	public Node(String name) {
		this.name = name;
	}

	public Node(String name, List<NodeWritable> nodes) {
		this.name = name;
		this.nodes = nodes;
	}
	
	/**
	 * Calculates the simplified <b>PageRank</b> for a given node
	 * 
	 * @param nodes
	 * @return PageRank
	 */
	public double calculateSimplePageRank(int totalNodes) {
		double pageRank = 0.0;
		for (NodeWritable node : nodes) {
			pageRank += node.getPageRank();
		}
		if (previousRank == 0.0) {
			rank = BigDecimal.valueOf(pageRank)
					.divide(BigDecimal.valueOf(totalNodes), PRECISION, RoundingMode.HALF_UP).doubleValue();
		} else {
			rank = BigDecimal.valueOf(pageRank).setScale(PRECISION, RoundingMode.HALF_UP).doubleValue();
			converged = (rank == previousRank);
		}
		return rank;
	}
	
	/**
	 * Calculates the complete <b>PageRank</b> for a given node. <br>
	 * This method makes use of the damping factor.<br>
	 * The equations is: <i>(1-d)/N + d (PR(T1)/C(T1) + ... +
	 * PR(Tn)/C(Tn))</i>. <br>
	 * As seen in Wikipedia:
	 * http://en.wikipedia.org/wiki/PageRank#Damping_factor
	 * 
	 * @param nodes
	 * @return PageRank
	 */
	public double calculateCompletePageRank(int totalNodes) {
		double pageRank = (Double.valueOf(1) - DAMPING_FACTOR.doubleValue()) / Double.valueOf(totalNodes);
		pageRank += (DAMPING_FACTOR * calculateSimplePageRank(totalNodes));
		rank = BigDecimal.valueOf(pageRank).setScale(PRECISION, RoundingMode.HALF_UP).doubleValue();
		converged = (rank == previousRank);
		return rank;
	}

	@Override
	public int compareTo(Node o) {
		return -1*Double.valueOf(rank).compareTo(Double.valueOf(o.rank));
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Node))
			return false;
		Node other = (Node) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

}
