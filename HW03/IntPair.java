package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {

	private int airline;
	private int month;

	public IntPair() {
		this.airline = 0;
		this.month = 0;
	}

	//set the airline and month
	public IntPair(int airline, int month) {
		this.airline = airline;
		this.month = month;
	}

	@Override
	//compare the airlines
	public int compareTo(IntPair other) {
		if (airline == other.airline) {
			return 0;
		}
		return airline > other.airline ? 1 : -1;
	}
	
    //compare the months
	public int monthCompare(IntPair other) {
		if (month == other.month) {
			return 0;
		}
		return month > other.month ? 1 : -1;
	}
    //check if the airline and month (each key) is equal
	public boolean equals(IntPair other) {
		return (airline == other.airline) && (month == other.month);
	}

	public int getAirline() {
		return airline;
	}

	public int getMonth() {
		return month;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		airline = in.readInt();
		month = in.readInt();
	}

	//set the airline
	public void setAirline(int airline) {
		this.airline = airline;
	}
    //set the month
	public void setMonth(int month) {
		this.month = month;
	}

	@Override
	public String toString() {
		return airline + ", " + month;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(airline);
		out.writeInt(month);
	}
}