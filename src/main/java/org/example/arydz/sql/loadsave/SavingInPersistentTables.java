package org.example.arydz.sql.loadsave;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;


// DataFrames can also be saved as persistent tables into Hive metastore.
// existing Hive deployment is not necessary to use this feature.
// Spark will create a default local Hive metastore (using Derby) for you.
// Persistent tables will still exist as long as you maintain your connection to the same metastore
// If no custom table path is specified, Spark will write data to a default table path under the warehouse directory.
public class SavingInPersistentTables implements SparkTask {

    @Override
    public void execute(SparkSession spark) {

        Logger log = Logger.getLogger("org");
        log.setLevel(Level.OFF);

        String path = "workspace/learning/Spark-Learning/src/main/resources/people.json";
        Dataset<Row> peopleDF = spark.read()
                .format("json")
                .load(path);

        System.out.println("About to Bucketing, Sorting");

        peopleDF.write()
                .bucketBy(42, "name")
                .sortBy("age")
                .saveAsTable("people_bucket");

        // bucketBy distributes data across a fixed number of buckets and
        // can be used when the number of unique values is unbounded.
        Dataset<Row> people_bucket = spark.table("people_bucket");
        people_bucket.show();

        // partitionBy creates a directory structure
        System.out.println("About to Partitioning");

        peopleDF.write()
                .partitionBy("name")
                .format("parquet")
                .save("partByName.parquet");

        System.out.println("Bucketing and Partitioning");

        peopleDF.write()
                .partitionBy("age")
                .bucketBy(42, "name")
                .saveAsTable("people_partitioned_bucketed");

    }
}
