package org.example.arydz.sql.loadsave;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

public class LoadAndSaveCSVSimple implements SparkTask {

    @Override
    public void execute(SparkSession spark) {
        Dataset<Row> peopleDF = spark.read()
                .format("csv")
                .option("sep", ";")
                // By setting inferSchema=true, Spark will automatically go through the csv file and infer the schema of each column.
                // It's being slower, but in return the dataframe will most likely have a correct schema given its input.
                .option("inferSchema", "true")
                .option("header", "true")
                .load("workspace/learning/Spark-Learning/src/main/resources/people.csv");

        peopleDF.printSchema();
        peopleDF.show(false);

    }
}
