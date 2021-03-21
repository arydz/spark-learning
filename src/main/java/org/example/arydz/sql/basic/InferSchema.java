package org.example.arydz.sql.basic;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.example.arydz.SparkTask;
import org.example.arydz.sql.Bar;

// This method uses reflection to infer the schema of an RDD
// that contains specific types of objects.
// This reflection-based approach leads to more concise code and
// works well when you already know the schema while writing your Spark application.
public class InferSchema implements SparkTask {

    private static final String FILE_NAME = "/home/arydz/stock_tests/5_aapl.us.txt";

    @Override
    public void execute(SparkSession spark) {

        readTextFileAsCSV(spark);

//        Logger log = Logger.getLogger("org");
//        log.setLevel(Level.OFF);

        readTextFile(spark);
    }

    private void readTextFileAsCSV(SparkSession spark) {
        Dataset<Row> csv = spark.read()
                .option("header", "true")   // turn of header
//                .option("header", "false")
                .option("delimiter", ",")
                .csv(FILE_NAME);

        csv.printSchema();
    }

    private void readTextFile(SparkSession spark) {
        Dataset<String> stringDataset = spark.read()
                .option("header", "false")
                .textFile(FILE_NAME);
        String first = stringDataset.first();

        JavaRDD<Bar> barRDD = stringDataset
                .javaRDD()
                // instead of this filter and map, use this mapPartitionWithIndex(...)
                .filter(value -> !value.equals(first))
                .map(line -> {
                    String[] parts = line.split(",");
                    Bar bar = new Bar();
                    bar.setTicket(parts[0]);
                    bar.setOpen(Double.parseDouble(parts[4].trim()));
                    bar.setClose(Double.parseDouble(parts[7].trim()));
                    return bar;
                });

        Dataset<Row> barDF = spark.createDataFrame(barRDD, Bar.class);
        barDF.createOrReplaceTempView("bar");

        Dataset<Row> closeHigherThenOpenDF = spark
                .sql("SELECT open, close FROM bar WHERE close > open");

        Encoder<String> stringEncoder = Encoders.STRING();
        closeHigherThenOpenDF
                .map((MapFunction<Row, String>) row -> "Open: " + row.getString(0) + " Close: " + row.getString(1),
                        stringEncoder);

        closeHigherThenOpenDF.show();

        System.out.println("TOTAL : " + barDF.count());
        System.out.println("HIGHER : " +  closeHigherThenOpenDF.count());
        System.out.println("LOWER : " + (barDF.count() - closeHigherThenOpenDF.count()));
    }
}
