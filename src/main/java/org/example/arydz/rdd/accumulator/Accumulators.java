package org.example.arydz.rdd.accumulator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.example.arydz.SparkTask;

import java.util.Arrays;

public class Accumulators implements SparkTask {

  @Override
  public void execute(SparkSession spark) {

    try (JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
      LongAccumulator accum = jsc.sc()
              .longAccumulator();
      jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6))
              .foreach(v -> accum.add(v));

      System.out.println(accum.value());
    }
  }
}
