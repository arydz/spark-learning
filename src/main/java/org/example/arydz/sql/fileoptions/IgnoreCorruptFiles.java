package org.example.arydz.sql.fileoptions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

public class IgnoreCorruptFiles implements SparkTask {

    @Override
    public void execute(SparkSession spark) {
        spark.sql("set spark.sql.files.ignoreCorruptFiles=true");
        // Missing file really means the deleted file under directory after you construct the DataFrame.
        spark.sql("set spark.sql.files.ignoreMissingFiles=true");

        Dataset<Row> testCorruptDF = spark.read()
                .parquet(
                        "workspace/learning/Spark-Learning/src/main/resources/dir1/",
                        "workspace/learning/Spark-Learning/src/main/resources/dir1/dir2/");

        testCorruptDF.show();
    }
}
