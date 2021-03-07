package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class Pivot {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir","c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("testingSql")
				.master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		
		dataset = dataset.select(col("level"),
								 date_format(col("datetime"),"MMMM").alias("month"),
								 date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
		
		Object[] months = new Object[] {"January", "February", "March", "April", "May", "June",
										"July", "August", "September", "October", "November", "December"};
		List<Object> columns = Arrays.asList(months);
		
		
		dataset = dataset.groupBy(col("level")).pivot("month", columns).count().na().fill(0);
		
		
		dataset.show(100);
								

	}

}
