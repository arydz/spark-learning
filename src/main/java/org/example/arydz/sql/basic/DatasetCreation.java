package org.example.arydz.sql.basic;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;
import org.example.arydz.sql.Bar;

import java.util.Arrays;
import java.util.Collections;

// It would be better to find some json file
public class DatasetCreation implements SparkTask {

    @Override
    public void execute(SparkSession spark) {
        Bar bar = new Bar();
        bar.setTicket("aapl");
        bar.setOpen(125);
        bar.setClose(115);

        Encoder<Bar> barEncoder = Encoders.bean(Bar.class);
        Dataset<Bar> javaBeanDS = spark.createDataset(Collections.singletonList(bar), barEncoder);

        javaBeanDS.printSchema();
        javaBeanDS.show();

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS =
                primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1, integerEncoder);
        // without this casting, couldn't build
        Integer[] collect = (Integer[]) transformedDS.collect();
        System.out.println(Arrays.toString(collect));

//        String path = "examples/src/main/resources/people.json";
//        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
//        peopleDS.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
    }
}
