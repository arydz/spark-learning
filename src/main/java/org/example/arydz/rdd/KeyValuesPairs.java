package org.example.arydz.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;
import scala.Tuple2;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class KeyValuesPairs implements SparkTask {

    @Override
    public void execute(SparkSession spark) {

        List<Integer> list = new Random().ints(0, 10)
                .limit(25)
                .boxed()
                .collect(Collectors.toList());

        try (JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
            JavaRDD<Integer> rdd = sc.parallelize(list);

            // Immediately return a FutureAction to the caller instead of
            // blocking on completion of the action.
            // This can be used to manage or wait for the asynchronous execution of the action.
            rdd.foreachAsync(System.out::println);

            JavaPairRDD<Integer, Integer> pairs = rdd.mapToPair(value -> new Tuple2<>(value, 1));
            JavaPairRDD<Integer, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
            counts.sortByKey()
                    .take(25)
                    .forEach(System.out::println);

            sc.stop();
        }
    }
}
