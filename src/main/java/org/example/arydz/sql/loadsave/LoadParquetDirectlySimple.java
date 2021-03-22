package org.example.arydz.sql.loadsave;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

public class LoadParquetDirectlySimple implements SparkTask {

    @Override
    public void execute(SparkSession spark) {
        String path = "`workspace/learning/Spark-Learning/src/main/resources/users.parquet`";
        Dataset<Row> result = spark.sql("SELECT * FROM parquet." + path);
        result.show();

    }
}
