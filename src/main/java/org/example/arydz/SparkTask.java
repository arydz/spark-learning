package org.example.arydz;

import org.apache.spark.sql.SparkSession;

public interface SparkTask {
    void execute(SparkSession spark);
}
