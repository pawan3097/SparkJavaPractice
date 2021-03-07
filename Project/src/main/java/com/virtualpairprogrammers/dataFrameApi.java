package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.text.SimpleDateFormat;
import java.util.Scanner;

public class dataFrameApi {
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir","c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("testingSql")
				.master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		SimpleDateFormat input = new SimpleDateFormat("MMMM");
		SimpleDateFormat output = new SimpleDateFormat("M");
		
		spark.udf().register("monthNum", (String month) -> {
			java.util.Date inputDate =  input.parse(month);
			return Integer.parseInt(output.format(inputDate));
		}, DataTypes.IntegerType);
		
		
//		dataset.createOrReplaceTempView("logs");	
//		Dataset<Row> results = spark.sql
//		("Select count(1) as total,level,date_format(dateTime, 'MMMM') as month from logs group by level,month order by monthNum(month),level");
		
		
//		results.show(100);
		
		dataset = dataset.select(col("level"),
								 date_format(col("datetime"),"MMMM").alias("month"),
								 date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
		
		dataset = dataset.groupBy(col("level"),col("month"),col("monthnum")).count();
		dataset = dataset.orderBy("monthnum","level");
		dataset = dataset.drop("monthnum");
		
		dataset.show(100);
		Scanner s = new Scanner(System.in);
		s.nextLine();
								 
		spark.close();
		

	}

}
