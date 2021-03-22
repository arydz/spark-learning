package org.example.arydz.sql.parquet;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

public class ReadParquet implements SparkTask {

    @Override
    public void execute(SparkSession spark) {
        String path = "workspace/learning/Spark-Learning/src/main/resources/people.json";

        Dataset<Row> peopleDF = spark.read()
                .json(path);

        peopleDF.write()
                .parquet("people.parquet");

        Dataset<Row> parquetFileDF = spark.read()
                .parquet("people.parquet");

        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql("SELECT name, 'Male' as gender FROM parquetFile WHERE age BETWEEN 13 and 19");
        namesDF.map((MapFunction<Row, String>) row -> "Name " + row.getString(0), Encoders.STRING());

        namesDF.write()
                .partitionBy("gender")
                .save("people_with_gender.parquet");

        namesDF.printSchema();
        namesDF.show();
    }
}
