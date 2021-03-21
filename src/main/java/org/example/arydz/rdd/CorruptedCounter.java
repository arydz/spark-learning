package org.example.arydz.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

// Will not work. Don't do like this
// The behavior of the above code is undefined, and may not work as intended.
// To execute jobs, Spark breaks up the processing of RDD operations into tasks,
// each of which is executed by an executor.
// Prior to execution, Spark computes the task’s closure.
// The closure is those variables and methods which must be visible for
// the executor to perform its computations on the RDD (in this case foreach()).
// This closure is serialized and sent to each executor.
public class CorruptedCounter implements SparkTask {

  @Override
  public void execute(SparkSession spark) {


    // The variables within the closure sent to each executor are now copies and thus,
    // when counter is referenced within the foreach function,
    // it’s no longer the counter on the driver node.
    // There is still a counter in the memory of the driver node
    // but this is no longer visible to the executors!
    // The executors only see the copy from the serialized closure.
    // Thus, the final value of counter will still be zero since all operations on counter
    // were referencing the value within the serialized closure.
    int counter[] = new int[] {0};
    List<Integer> list = new Random().ints(0, 50).limit(10).boxed().collect(Collectors.toList());

    try (JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
      JavaRDD<Integer> rdd = sc.parallelize(list, 1);
      rdd.foreach(x -> counter[0] += x);

      System.out.println("Counter value: " + counter[0]);

      sc.stop();
    }
  }
}
