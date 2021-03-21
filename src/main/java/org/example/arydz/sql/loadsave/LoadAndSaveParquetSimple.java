package org.example.arydz.sql.loadsave;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

public class LoadAndSaveParquetSimple implements SparkTask {

    @Override
    public void execute(SparkSession spark) {
        Dataset<Row> usersDF = spark.read()
                .load("workspace/learning/Spark-Learning/src/main/resources/users.parquet");

        usersDF.select("name", "favorite_color")
                .write()
                .save("namesAndFavColors.parquet");
    }
}
