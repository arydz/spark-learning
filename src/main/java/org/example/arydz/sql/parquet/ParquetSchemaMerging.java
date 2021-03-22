package org.example.arydz.sql.parquet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

import java.util.ArrayList;
import java.util.List;

public class ParquetSchemaMerging implements SparkTask {

    @Override
    public void execute(SparkSession spark) {

        List<Square> squares = new ArrayList<>();
        for (int value = 1; value <= 5; value++) {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            square.setMessage("square:"+value);
            squares.add(square);
        }

        // Create a simple DataFrame, store into a partition directory
        Dataset<Row> squareDF = spark.createDataFrame(squares, Square.class);
        squareDF.write()
                .parquet("data/test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        for (int value = 6; value <= 10; value++) {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setCube(value * value * value);
            cubes.add(cube);
        }

        // Create another DataFrame in a new partition directory,
        // adding a new column and dropping an existing column
        Dataset<Row> cubeDF = spark.createDataFrame(cubes, Cube.class);
        cubeDF.write()
                .parquet("data/test_table/key=2");

        Dataset<Row> mergedDF = spark.read()
                .option("mergeSchema", true)
                .parquet("data/test_table");

        mergedDF.printSchema();
        mergedDF.show();

    }
}
