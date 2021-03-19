package org.example.arydz;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class ReadWholeTextFiles implements SparkTask {

  private static final String FILE_NAME = "/home/arydz/stock_tests/*.txt";

  @Override
  public void execute(SparkSession sparkSession) {
    RDD<Tuple2<String, String>> textData =
        sparkSession.sparkContext().wholeTextFiles(FILE_NAME, 1).cache();
    JavaRDD<Tuple2<String, String>> rdd = textData.toJavaRDD();

    rdd.foreach((VoidFunction<Tuple2<String, String>>) line -> System.out.println(line));

    long lines = textData.count();
    System.out.println("Total number of Lines : " + lines);

    sparkSession.stop();
  }
}
