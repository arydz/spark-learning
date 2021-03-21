package org.example.arydz.sql.fileoptions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

public class RecursiveFileLookup implements SparkTask {

    @Override
    public void execute(SparkSession spark) {

        spark.sql("set spark.sql.files.ignoreCorruptFiles=true");

        Dataset<Row> df = spark.read().format("parquet")
//                .option("pathGlobFilter", "*.parquet")
                .option("recursiveFileLookup", "true")
                .load("workspace/learning/Spark-Learning/src/main/resources/dir1");

        df.show();

    }
}
