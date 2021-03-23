package org.example.arydz.sql.jdbc;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.example.arydz.SparkTask;

import java.util.Properties;

// Spark Dataframes are immutable structure.
// So the way to update dataframe is to merge the older dataframe and the newer dataframe and save the merged dataframe on HDFS.
// To update the older ID you would require some de-duplication key (Timestamp may be).
public class JdbcInsertStock implements SparkTask {

    private static final String FILE_NAME = "/home/arydz/stock_tests/5_aapl.us.txt";

    @Override
    public void execute(SparkSession spark) {

        Dataset<Row> appleStock = spark.read()
                .option("header", true)
                .option("delimiter", ",")
                .csv(FILE_NAME);

        appleStock.show();

        Encoder<StockEntity> stockEncoder = Encoders.bean(StockEntity.class);
        Dataset<StockEntity> stockDataset = appleStock.map((MapFunction<Row, StockEntity>) row -> {
            StockEntity stockEntity = new StockEntity();
            stockEntity.setTicket(String.valueOf(row.get(0)));
            stockEntity.setAvg_volume(1d);
            return stockEntity;
        }, stockEncoder);

        stockDataset.printSchema();
        stockDataset.show();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "spark");
        connectionProperties.put("password", "password");
        stockDataset.select("ticket")
                .write()
                .mode(SaveMode.Append)
                .jdbc("jdbc:postgresql://localhost:5432/spark_learning", "public.STOCK", connectionProperties);

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/spark_learning")
                .option("dbtable", "public.STOCK")
                .option("user", "spark")
                .option("password", "password")
                .load();

        jdbcDF.explain();
        jdbcDF.show();
    }
}
