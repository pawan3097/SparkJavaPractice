package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class testingJoins {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir","c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("TestingJoins").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List <Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
		visitsRaw.add(new Tuple2<>(4,18));
		visitsRaw.add(new Tuple2<>(6,4));
		visitsRaw.add(new Tuple2<>(10,9));
		
		List <Tuple2 <Integer,String>> usersRaw = new ArrayList<>();
		usersRaw.add(new Tuple2<>(1,"Professor"));
		usersRaw.add(new Tuple2<>(2,"Berlin"));
		usersRaw.add(new Tuple2<>(3,"Rio"));
		usersRaw.add(new Tuple2<>(4,"Tokyo"));
		usersRaw.add(new Tuple2<>(5,"Stockholm"));
		usersRaw.add(new Tuple2<>(6,"Denver"));
		
		JavaPairRDD<Integer,Integer> visits = sc.parallelizePairs(visitsRaw);
		JavaPairRDD<Integer,String> users = sc.parallelizePairs(usersRaw);
		
		JavaPairRDD<Integer,Tuple2<Integer,String>> joinedInner = visits.join(users) ;
		
		JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedLeft = visits.leftOuterJoin(users);
		
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRight = visits.rightOuterJoin(users);
		
		 JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedCartesian = visits.cartesian(users);
		
//		joinedInner.collect().forEach(System.out::println);
//		
//		joinedLeft.foreach(it -> System.out.println(it._2._2.orElse("blank").toUpperCase()));
//		
//		joinedRight.foreach(it -> System.out.println("User " + it._2._2 + " had " + it._2._1.orElse(0) + " visits"));
		 
		joinedCartesian.collect().forEach(System.out::println);
		
		sc.close();
		

	}

}
