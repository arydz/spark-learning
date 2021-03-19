package org.example.arydz;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class GenerateRandomLongValues implements SparkTask {

  @Override
  public void execute(SparkSession sparkSession) {

    Logger log = Logger.getLogger("org");

    List<Long> list =
        new Random().longs(-500, 4900).limit(3500).boxed().collect(Collectors.toList());

    try (JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext())) {
      JavaRDD<Long> rdd = sparkContext.parallelize(list);
      rdd.foreach((VoidFunction<Long>) value -> System.out.println("Java RDD : " + value));
      Long reduce = rdd.reduce((v1, v2) -> v1 + v2);

      log.info("TOTAL SUM " + reduce);

      Thread.sleep(10000);
      sparkContext.stop();
    } catch (InterruptedException e) {
      System.out.println(e.getMessage());
    } finally{
      log.info("Task completed: " + GenerateRandomLongValues.class.getSimpleName());
    }
  }
}
