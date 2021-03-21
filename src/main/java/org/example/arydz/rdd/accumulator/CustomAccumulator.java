package org.example.arydz.rdd.accumulator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

import java.util.Arrays;

public class CustomAccumulator implements SparkTask {

    @Override
    public void execute(SparkSession spark) {

        try (JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
            PersonAccumulatorV2 personAccum = new PersonAccumulatorV2();
            jsc.sc()
                    .register(personAccum, "PersonAcc1");

            jsc.parallelize(Arrays.asList(new Person(25)))
                    .foreach(v -> personAccum.add(v));

            System.out.println(personAccum.value());
        }
    }
}
