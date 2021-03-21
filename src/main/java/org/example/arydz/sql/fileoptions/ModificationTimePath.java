package org.example.arydz.sql.fileoptions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

public class ModificationTimePath implements SparkTask {

    @Override
    public void execute(SparkSession spark) {

        Dataset<Row> df = spark.read()
                .format("parquet")
                .option("pathGlobFilter", "*.parquet")
                .option("modifiedBefore", "2021-02-22T03:35:00")
                .option("modifiedAfter", "2021-02-21T03:35:00")
                .option("timeZone", "CST")
                .load("workspace/learning/Spark-Learning/src/main/resources/dir1");

        df.show();
    }
}
