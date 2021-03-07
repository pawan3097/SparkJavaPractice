package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(35);
		inputData.add(90);
		inputData.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> myRdd = sc.parallelize(inputData);
		
		Integer result = myRdd.reduce( (value1, value2) -> value1 + value2 );
		
		JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value) );
		
		JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
		
		Long count = singleIntegerRdd.reduce( (value1,value2) -> value1 + value2);
		
		JavaRDD<Tuple2<Integer, Double>> sqrtRddTuple = myRdd.map(value -> new Tuple2<> (value, Math.sqrt(value)));
		
		//Tuple2<Integer, Double> myValue =  new Tuple2<> (9,3.0);
		
		//System.out.println(sqrtRddTuple);
		
		System.out.println(count);
		
		sqrtRddTuple.collect().forEach( System.out::println );
		
		System.out.println(result);
		
		sc.close();
		

	}

}
