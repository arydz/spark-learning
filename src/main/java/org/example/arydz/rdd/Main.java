package org.example.arydz.rdd;

import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;
import org.example.arydz.rdd.accumulator.CustomAccumulator;

public class Main {

  public static void main(String... args) {
    SparkSession sparkSession =
        SparkSession.builder().appName("Spark-Learning").master("local[*]").getOrCreate();

    SparkTask sparkTask = new CustomAccumulator();
    sparkTask.execute(sparkSession);
  }
}
