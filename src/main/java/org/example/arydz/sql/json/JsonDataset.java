package org.example.arydz.sql.json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.arydz.SparkTask;

import java.util.Arrays;
import java.util.List;

// Note that the file that is offered as a json file is not a typical JSON file.
// Each line must contain a separate, self-contained valid JSON object.
public class JsonDataset implements SparkTask {
    
    @Override
    public void execute(SparkSession spark) {

        String path = "workspace/learning/Spark-Learning/src/main/resources/people.json";

        Dataset<Row> people = spark.read()
                // for regular json files
//                .option("multiLine", true)
                .json(path);

        people.printSchema();

        people.createOrReplaceTempView("people");

        Dataset<Row> nameDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        nameDF.show();

        List<String> jsonData = Arrays.asList("name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> anotherPeople = spark.read()
                .json(anotherPeopleDataset);

        anotherPeople.show(false);
    }
}
