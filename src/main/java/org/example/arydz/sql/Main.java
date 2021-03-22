package org.example.arydz.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Java Spark SQ")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

//        new BasicDataFrame().execute(spark);
//        new DatasetCreation().execute(spark);
//        new InferSchema().execute(spark);
//        new ProgrammaticSchema().execute(spark);

//        new LoadAndSaveParquetSimple().execute(spark);
//        new LoadAndSaveJsonSimple().execute(spark);
//        new LoadAndSaveCSVSimple().execute(spark);
//        new LoadParquetDirectlySimple().execute(spark);
//        new SavingInPersistentTables().execute(spark);
//        new IgnoreCorruptFiles().execute(spark);
//        new PathGlobalFilter().execute(spark);
//        new RecursiveFileLookup().execute(spark);
//        new ModificationTimePath().execute(spark);
//        new ReadParquet().execute(spark);
//        new ParquetSchemaMerging().execute(spark);
//        new JsonDataset().execute(spark);

    }
}
