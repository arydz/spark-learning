package org.example.arydz.sql.basic;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.arydz.SparkTask;

import java.util.ArrayList;
import java.util.List;

// This method for creating Datasets is through a programmatic interface that allows
// you to construct a schema and then apply it to an existing RDD.
// While this method is more verbose,
// it allows you to construct Datasets when the columns and their types are not known until runtime.
public class ProgrammaticSchema implements SparkTask {

    private static final String FILE_NAME = "/home/arydz/stock_tests/5_aapl.us.txt";

    @Override
    public void execute(SparkSession spark) {

        JavaRDD<String> barRDD = spark.read()
                .textFile(FILE_NAME)
                .javaRDD().filter(value -> !value.contains("<TICKER>"));

        String schemaString = "TICKER PER DATE TIME OPEN HIGH LOW CLOSE VOL OPENINT";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = barRDD.map(record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1], attributes[2], attributes[3], attributes[4],
                    attributes[5], attributes[6], attributes[7], attributes[8], attributes[9]);
        });

        Dataset<Row> barDataFrame = spark.createDataFrame(rowRDD, schema);
        barDataFrame.createOrReplaceTempView("bar");

        Dataset<Row> results = spark.sql("SELECT TICKER, DATE FROM bar");

        Dataset<String> basicDS = results.map((MapFunction<Row, String>) row ->
                "TICKER: " + row.getString(0) + " DATE: " + row.getString(1), Encoders.STRING());
        basicDS.show(false);
    }
}
