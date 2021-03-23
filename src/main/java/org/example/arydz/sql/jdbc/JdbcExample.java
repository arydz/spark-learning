package org.example.arydz.sql.jdbc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

import java.util.Properties;

public class JdbcExample implements SparkTask {

    @Override
    public void execute(SparkSession spark) {

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/spark_learning")
                .option("dbtable", "public.STOCK")
                .option("user", "spark")
                .option("password", "password")
                // optional for this example. Jar file directly added to spark (jars dir)
//                .option("driver", "org.postgresql.Driver")
                .load();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "spark");
        connectionProperties.put("password", "password");
//        connectionProperties.setProperty("driver", "org.postgresql.Driver");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:postgresql://localhost/spark_learning", "public.STOCK", connectionProperties);

//        jdbcDF.write()
//                .format("jdbc")
//                .option("url", "jdbc:postgresql://localhost:5432/spark_learning")
//                .option("dbtable", "public.STOCK")
//                .option("user", "spark")
//                .option("password", "password")
//                .save();
//
//        jdbcDF2.write()
//                .jdbc("jdbc:postgresql://localhost:5432/spark_learning", "public.STOCK", connectionProperties);

//        jdbcDF.write()
//                .option("createTableColumnTypes", "name VARCHAR(64), comments VARCHAR(512)")
//                .jdbc("jdbc:postgresql://localhost:5432/spark_learning", "public.USER_COMMENTS", connectionProperties);

        jdbcDF.explain();

        jdbcDF2.show();
    }
}
