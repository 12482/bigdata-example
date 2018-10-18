package com.example.spark;

import java.util.TreeSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkTopN {

	public static void main(String[] args) {
		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
		int topN = 3;
		SparkConf conf = new SparkConf().setAppName("topN").setMaster("local[4]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("score");
		JavaPairRDD<String, String> classrdd = lines.mapToPair(line -> {
			String[] splited = line.split(",");
			return new Tuple2<>(splited[0], line);
		});

		classrdd.groupByKey().map(oneClass -> {
			String className = oneClass._1;
			//降序排序的treeset，只保留topN个元素
//			TreeSet<String> topSet = new TreeSet<String>(new MyDescCompare<String>());
			TreeSet<String> topSet =	new TreeSet<String>(( String x,String y)->  {
				int score1 = Integer.valueOf(x.toString().split(",")[2]);
				int score2 = Integer.valueOf(y.toString().split(",")[2]);
				return score2 -score1;
			});
			for (String str : oneClass._2) {
				topSet.add(str);
				if (topSet.size() > topN) {
					topSet.pollLast();
				}
			}
			return topSet;
		}).foreachPartition(s -> s.forEachRemaining(System.out :: println));
		//foreach ，每个元素执行一次
		//foreachPartition , 每个分区执行一次
		jsc.stop();

	}

}
