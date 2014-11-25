package com.hadoop.designpatterns;

import java.util.Iterator;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * Wrapper class to the MapWritable. Overriding the toString() method in order
 * to print into the file.
 * 
 * @author pmonteiro
 *
 */
public class StripesMapWritable extends MapWritable implements Writable {

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Iterator<Writable> keys = this.keySet().iterator();
		Iterator<Writable> values = this.values().iterator();
		while (keys.hasNext()) {
			Writable key = keys.next();
			Writable value = values.next();
			sb.append(key + ":" + value + " ");
		}
		return sb.toString();
	}
}
