package de.unikoblenz.west.lkastler.uriposition;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * creates mappings from incoming lines of text to URI and Position items. 
 * 
 * @author lkastler
 */
public class UriPositionMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Position>{

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Position> collect, Reporter report)
			throws IOException {
		
		String[] items = value.toString().split(" ");
		
		if(items.length == 2) {
			collect.collect(new Text(items[0]), new Position(items[1]));
		}
	}
}
