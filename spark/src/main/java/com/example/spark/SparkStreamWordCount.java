package com.example.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamWordCount {	
	
	public static void main(String[] args) throws InterruptedException {
		java_lamada();
	}
	
	/**
	 * 接口式
	 * @throws InterruptedException
	 */
	private static void java_interface() throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnline");
        /**
         * 在创建streaminContext的时候 设置batch Interval
         */
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 9999);//监控socket端口9999
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
        });

        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String t) throws Exception {
				  return new Tuple2<String, Integer>(t, 1);
			}
        });
        JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
		                return v1 + v2;
			}
        });
         
         counts.print();
         jsc.start();
         //等待spark程序被终止
         jsc.awaitTermination();
         jsc.stop(false);
	}
	
	
	public static void java_lamada() throws InterruptedException {

        
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnline");
        /**
         * 在创建streaminContext的时候 设置batch Interval
         */
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 9999);//监控socket端口9999
        //压平
        JavaDStream<String> words =    lines.flatMap( t ->Arrays.asList(t.split(" ")).iterator());
        //转换成pair
        JavaPairDStream<String, Integer> ones =  words.mapToPair(t -> new Tuple2<>(t, 1));
        //根据key聚合
        JavaPairDStream<String, Integer> counts =   ones.reduceByKey((v1,v2) -> v1+v2);
        //翻转
        JavaPairDStream<Integer, String> countwords =  counts.mapToPair(w -> new Tuple2<>(w._2,w. _1));
        //降序排列、翻转
        JavaPairDStream<String, Integer> result =   countwords.transformToPair(rdd -> rdd.sortByKey(false)).mapToPair(w -> new Tuple2<>(w._2,w. _1));
        
        //outputoperator类的算子   
        result.print();
         jsc.start();
         //等待spark程序被终止
         jsc.awaitTermination();
         jsc.stop(false);
    // spark spark study new study spark
	}

}
