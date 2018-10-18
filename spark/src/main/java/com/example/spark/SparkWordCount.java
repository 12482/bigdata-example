package com.example.spark;

import java.util.Comparator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.bean.SecondarySort;

import scala.Tuple2;

public class SparkWordCount {

	public static void main(String[] args) throws InterruptedException {
		 Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[4]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("secondarySort");
		JavaPairRDD<SecondarySort, String> sortline = lines.mapToPair(line -> {
			String[] splited = line.split(" ");
			SecondarySort sortKey = new SecondarySort(Integer.valueOf(splited[0]), Integer.valueOf(splited[1]));
			return new Tuple2<>(sortKey, line);
		});
		//！！设置分区为1，否则只有分区中的数据具有顺序
		sortline.sortByKey(true,1).map(s -> s._2).foreach(v -> System.err.println(v));
		jsc.close();
	}

}
