package de.unikoblenz.west.lkastler.uriposition;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * aggregates up all positions of a given URI to a PositionCount item.  
 * 
 * @author lkastler
 */
public class UriPositionReducer extends MapReduceBase implements Reducer<Text,Position,Text,PositionCounts>{

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void reduce(Text key, Iterator<Position> values,
			OutputCollector<Text, PositionCounts> collect, Reporter report)
			throws IOException {
		PositionCounts counts = new PositionCounts();
		
		while(values.hasNext()) {
			Position p = values.next();
			
			if(p.isSubject()) {
				counts.increaseSubjectCount();
			}
			else if(p.isPredicate()) {
				counts.increasePredicateCount();
			}
			else if(p.isObject()) {
				counts.increaseObjectCount();
			}
		}
		
		collect.collect(key, counts);
	}

}
