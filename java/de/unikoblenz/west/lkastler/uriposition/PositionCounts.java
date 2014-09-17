package de.unikoblenz.west.lkastler.uriposition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * implements the Writable interface to store the occurrences of a related URI for a certain position.
 * @author lkastler
 */
public class PositionCounts implements Writable {

	private long subjectCount = 0;
	private long predicateCount = 0;
	private long objectCount = 0;

	/**
	 * default constructor
	 */
	public PositionCounts() {}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(subjectCount);
		out.writeLong(predicateCount);
		out.writeLong(objectCount);

	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		subjectCount = in.readLong();
		predicateCount = in.readLong();
		objectCount = in.readLong();
	}

	/**
	 * returns the occurrence amount of the related URI in the subject of considered triples.
	 * @return the occurrence amount of the related URI in the subject of considered triples.
	 */
	public long getSubjectCount() {
		return subjectCount;
	}

	/**
	 * returns the occurrence amount of the related URI in the predicate of considered triples.
	 * @return the occurrence amount of the related URI in the predicate of considered triples.
	 */
	public long getPredicateCount() {
		return predicateCount;
	}

	/**
	 * returns the occurrence amount of the related URI in the object of considered triples.
	 * @return the occurrence amount of the related URI in the object of considered triples.
	 */
	public long getObjectCount() {
		return objectCount;
	}

	/**
	 * returns the total occurrence amount of the related URI in considered triples.
	 * @return the total occurrence amount of the related URI in considered triples.
	 */
	public long getTotalCount() {
		return subjectCount + predicateCount + objectCount;
	}

	/**
	 * increases the counter for the subject-related occurrence.
	 */
	public void increaseSubjectCount() {
		subjectCount++;
	}
	
	/**
	 * increases the counter for the predicate-related occurrence.
	 */
	public void increasePredicateCount() {
		predicateCount++;
	}
	
	/**
	 * increases the counter for the object-related occurrence.
	 */
	public void increaseObjectCount() {
		objectCount++;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return subjectCount+ "\t" + predicateCount + "\t"+ objectCount + "\t" + getTotalCount();
	}
}
