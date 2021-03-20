package org.example.arydz;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class BroadcastVariables implements SparkTask {

    @Override
    public void execute(SparkSession sparkSession) {

        try (JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext())) {

            Broadcast<int[]> broadcastVar = sc.broadcast(new int[]{1, 2, 3});
            // After the broadcast variable is created,
            // it should be used instead of the value v in any
            // functions run on the cluster so that v is not shipped to the nodes more than once.
            // In addition, the object v should not be modified after it is broadcast in order
            // to ensure that all nodes get the same value of the broadcast variable
            int[] value = broadcastVar.value();

            System.out.println(Arrays.toString(value));
            // The broadcast is used again afterwards, it will be re-broadcast.
            broadcastVar.unpersist();
            System.out.println(Arrays.toString(value));
            // The broadcast variable can’t be used after that.
            broadcastVar.destroy();
            System.out.println(Arrays.toString(value));

            sc.stop();
        }
    }
}
