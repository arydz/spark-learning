package org.example.arydz.sql.fileoptions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

public class PathGlobalFilter implements SparkTask {

    @Override
    public void execute(SparkSession spark) {
        spark.sql("set spark.sql.files.ignoreCorruptFiles=false");

        Dataset<Row> df = spark.read().format("parquet")
                //  Load files with paths matching a given glob pattern while keeping the behavior of partition discovery
                .option("pathGlobFilter", "*.parquet")
                .load("workspace/learning/Spark-Learning/src/main/resources/dir1");

        df.show();
    }
}
