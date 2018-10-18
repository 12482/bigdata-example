package com.example.spark.bean;

import java.io.Serializable;
import java.util.Comparator;

public class MyDescCompare<T> implements Comparator<T>, Serializable {
	
	@Override
	public int compare(T o1, T o2) {
		int score1 = Integer.valueOf(o1.toString().split(",")[2]);
		int score2 = Integer.valueOf(o2.toString().split(",")[2]);
		return score2 -score1;
	}
}
