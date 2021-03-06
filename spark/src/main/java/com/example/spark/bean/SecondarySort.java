package com.example.spark.bean;

import java.io.Serializable;

import scala.math.Ordered;

public class SecondarySort implements Ordered<SecondarySort>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int first;
	private int second;

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	public SecondarySort(int first, int second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public int compare(SecondarySort that) {
		if (this.first - that.getFirst() != 0) {
			return this.first - that.getFirst();
		} else {
			return this.second - that.getSecond();
		}

	}

	@Override
	public boolean $less(SecondarySort that) {
		if (this.first < that.getFirst()) {
			return true;
		} else if (this.first == that.getFirst() && this.second < that.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater(SecondarySort that) {

		if (this.first > that.getFirst()) {
			return true;
		} else if (this.first == that.getFirst() && this.second > that.second) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(SecondarySort that) {
		if (this.$less(that)) {
			return true;
		} else if (this.first == that.getFirst() && this.second == that.second) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(SecondarySort that) {
		if (this.$greater(that)) {
			return true;
		} else if (this.first == that.getFirst() && this.second == that.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public int compareTo(SecondarySort that) {
		if (this.first - that.getFirst() != 0) {
			return this.first - that.getFirst();
		} else {
			return this.second - that.getSecond();
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		SecondarySort that = (SecondarySort) o;

		if (first != that.first)
			return false;
		return second == that.second;

	}

	@Override
	public int hashCode() {
		int result = first;
		result = 31 * result + second;
		return result;
	}

}
