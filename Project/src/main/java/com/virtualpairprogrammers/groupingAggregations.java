package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class groupingAggregations {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir","c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("testingSql")
				.master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		dataset.createOrReplaceTempView("logs");
		
		Dataset<Row> results = spark.sql("Select count(1) as total,level,date_format(dateTime, 'MMMM') as month from logs group by 2,3");
		Dataset<Row> total = spark.sql("Select count(*) as total from logs");
		results.show(100);
		total.show();

	}	
}
