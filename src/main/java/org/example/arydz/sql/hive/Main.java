package org.example.arydz.sql.hive;

import org.apache.spark.sql.SparkSession;

import java.io.File;

public class Main {

    public static void main(String[] args) {

        // warehouseLocation points to the default location for managed databases and tables
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        new HiveExample().execute(spark);
    }
}
