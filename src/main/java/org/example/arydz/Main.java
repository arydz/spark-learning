package org.example.arydz;

import org.apache.spark.sql.SparkSession;
import org.example.arydz.accumulator.CustomAccumulator;

public class Main {

  public static void main(String... args) {
    SparkSession sparkSession =
        SparkSession.builder().appName("Spark-Learning").master("local[*]").getOrCreate();

    SparkTask sparkTask = new CustomAccumulator();
    sparkTask.execute(sparkSession);
  }
}
