package org.example.arydz.sql.hive;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

import java.util.ArrayList;
import java.util.List;

public class HiveExample implements SparkTask {

    @Override
    public void execute(SparkSession spark) {

        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
        spark.sql("LOAD DATA LOCAL INPATH 'workspace/learning/Spark-Learning/src/main/resources/kv1.txt' INTO TABLE src");

        spark.sql("SELECT * FROM src")
                .show(false);

        spark.sql("SELECT COUNT(*) FROM src")
                .show();

        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

        Dataset<String> stringsDS =
                sqlDF.map((MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
                        Encoders.STRING());

        stringsDS.show();

        List<Record> records = new ArrayList<>();
        for (int key = 1; key < 100; key++) {
            Record record = new Record();
            record.setKey(key);
            record.setValue("val_" + key);
            records.add(record);
        }

        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

        // Queries can then join DataFrames data with data stored in Hive.
        spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
    }
}
