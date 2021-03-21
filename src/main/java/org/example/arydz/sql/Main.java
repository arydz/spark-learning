package org.example.arydz.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {
        SparkConf conf= new SparkConf().setAppName("Java Spark SQ").setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

//        new BasicDataFrame().execute(spark);
//        new DatasetCreation().execute(spark);
//        new InferSchema().execute(spark);
        new ProgrammaticSchema().execute(spark);

    }
}
