package org.example.arydz.sql.basic;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

import static org.apache.spark.sql.functions.col;

// It would be better to find some json file
public class BasicDataFrame implements SparkTask {

    private static final String FILE_NAME = "/home/arydz/stock_tests/5_aapl.us.txt";

    // If using json it would be
    // df.printSchema();
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)
    //
    // and we could specify column
    // Select only the "name" column
    // df.select("name").show();
    //
    // Count people by age
    // df.groupBy("age").count().show();
    @Override
    public void execute(SparkSession spark) {

        Logger log = Logger.getLogger("org");
        log.setLevel(Level.WARN);

        Dataset<Row> dfFromCSV = spark.read()
                .option("header", true)
                .option("delimiter", ",")
                .csv(FILE_NAME).toDF("<TICKER>","<PER>","<DATE>","<TIME>","<OPEN>","<HIGH>","<LOW>","<CLOSE>","<VOL>","<OPENINT>");

        dfFromCSV.printSchema();
        dfFromCSV.show();

        Dataset<Row> df = spark.read()
                .textFile(FILE_NAME)
                .toDF();

        df.printSchema();

        df.select("value")
                .show();

        df.select(col("value").plus("Additional value"))
                .show(false);

        df.filter(col("value").contains("153000"))
                .show(false);

        df.groupBy("value")
                .count()
                .show();

        df.show(50, false);

        df.createOrReplaceTempView("lines");

        Dataset<Row> sql = spark.sql("SELECT * FROM lines");
        sql.show();

        try {
            df.createGlobalTempView("lines");

            spark.sql("SELECT * FROM global_temp.lines")
                    .show();

            spark.newSession()
                    .sql("SELECT * FROM global_temp.lines")
                    .show();

        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }

    }
}
