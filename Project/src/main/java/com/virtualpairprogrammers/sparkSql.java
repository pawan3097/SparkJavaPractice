package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class sparkSql {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir","c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// SparkConf conf = new SparkConf().setAppName("LearningPairRDDs").setMaster("local[*]");
		// JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder()
				.appName("testingSql")
				.master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
//		Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");
//		modernArtResults.show();
//		Dataset<Row> modernArtLambda = dataset.filter(row -> row.getAs("subject").equals("Modern Art")
//															&& Integer.parseInt(row.getAs("year")) >= 2007);
//		modernArtLambda.show();
		
		
		Dataset<Row> modernArtCol = dataset.filter(col("subject").equalTo("Modern Art")
																.and(col("year").geq(2007)));
		modernArtCol.show();
		
		spark.close();
	}

}
