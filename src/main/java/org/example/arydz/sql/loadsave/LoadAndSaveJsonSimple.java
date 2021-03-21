package org.example.arydz.sql.loadsave;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

public class LoadAndSaveJsonSimple implements SparkTask {

    @Override
    public void execute(SparkSession spark) {
        Dataset<Row> peopleDF = spark.read()
                .format("json")
                .load("workspace/learning/Spark-Learning/src/main/resources/people.json");

        peopleDF.select("name", "age")
                .write()
                .mode(SaveMode.Overwrite)
                .save("namesAndAges.parquet");
//                .format("json")
//                .save("namesAndAges.json");

    }
}
