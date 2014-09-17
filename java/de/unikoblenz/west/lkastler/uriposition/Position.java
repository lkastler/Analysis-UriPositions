package de.unikoblenz.west.lkastler.uriposition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * implementation of the Writable interface that indicates the position of the URI within a triple 
 * @author lkastler
 */
public class Position implements Writable {
	
	private boolean isSubject = false;
	private boolean isPredicate = false;
	private boolean isObject = false;
	
	/**
	 * default constructor
	 */
	public Position(){}
	
	/**
	 * constructor that takes the encoding of the position ("s","p", or "o") and creates a Position object.
	 * @param encoded - String-encoding of the URI position within the triple ("s", "o", or "p").
	 */
	public Position(String encoded) {
		if(encoded.trim().equalsIgnoreCase("s")) {
			isSubject = true;
		}
		else if(encoded.trim().equalsIgnoreCase("p")) {
			isPredicate = true;
		}
		else if(encoded.trim().equalsIgnoreCase("o")) {
			isObject = true;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isSubject);
		out.writeBoolean(isPredicate);
		out.writeBoolean(isObject);
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		isSubject = in.readBoolean();
		isPredicate = in.readBoolean();
		isObject = in.readBoolean();
	}

	/**
	 * returns if this Position indicates a subject position.
	 * @return if this Position indicates a subject position.
	 */
	public boolean isSubject() {
		return isSubject;
	}

	/**
	 * returns if this Position indicates a predicate position.
	 * @return if this Position indicates a predicate position.
	 */
	public boolean isPredicate() {
		return isPredicate;
	}


	/**
	 * returns if this Position indicates a object position.
	 * @return if this Position indicates a object position.
	 */
	public boolean isObject() {
		return isObject;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Position [" + isSubject + "," + isPredicate + "," + isObject + "]";
	}
}
