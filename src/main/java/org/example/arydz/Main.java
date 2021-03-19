package org.example.arydz;

import org.apache.spark.sql.SparkSession;

public class Main {

  public static void main(String... args) {
    SparkSession sparkSession =
        SparkSession.builder().appName("Spark-Learning").master("local[*]").getOrCreate();

    SparkTask sparkTask = new ReadTextFile();
    sparkTask.execute(sparkSession);
  }
}
