package com.hadoop.coursework2.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;

import org.apache.hadoop.io.WritableComparable;

/**
 * Wrapper class to the MapWritable. Overriding the toString() method in order
 * to print into the file.
 * 
 * @author pmonteiro
 *
 */

public class ListWritable implements WritableComparable<ListWritable> {

	private @Getter String key;
	private @Getter List<NodeWritable> nodes = new ArrayList<>();

	public ListWritable() {
		super();
	}
	
	public ListWritable(String key) {
		super();
		this.key = key;
	}
	
	public ListWritable add(NodeWritable node) {
		nodes.add(node);
		return this;
	}
	
	public NodeWritable get(Integer index) {
		return nodes.get(index);
	}

	public boolean contains(NodeWritable node) {
		return nodes.contains(node);
	}
	
	public ListWritable clear() {
		nodes.clear();
		return this;
	}
	
	@Override
	public int compareTo(ListWritable o) {
		return this.key.compareTo(o.getKey());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		clear();
		for (int i = 0; i < size; i++) {
			nodes.add(new NodeWritable(in.readUTF(), in.readUTF()));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(nodes.size());
		for (NodeWritable node : nodes) {
			out.writeUTF(node.getFrom());
			out.writeUTF(node.getTo());
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ListWritable))
			return false;
		ListWritable other = (ListWritable) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}
}
