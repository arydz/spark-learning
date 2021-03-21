package org.example.arydz.rdd;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.example.arydz.SparkTask;

public class ReadTextFile implements SparkTask {

  //  private static final String FILE_NAME = "/home/arydz/stock_tests/5_aapl.us.txt";
  private static final String FILE_NAME = "/home/arydz/stock_tests/*.txt";

  @Override
  public void execute(SparkSession spark) {
//    Dataset<String> textData = sparkSession.read().textFile(FILE_NAME).cache();
    Dataset<String> textData = spark.read().textFile(FILE_NAME).persist(StorageLevel.MEMORY_ONLY());
    textData.foreach((ForeachFunction<String>) line -> System.out.println(line));

    long lines = textData.count();
    System.out.println("Total number of Lines : " + lines);

    spark.stop();
  }
}
