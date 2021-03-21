# Based on Apache Spark official docs

| Link   |      Status     |
|----------|:-------------:|
| https://spark.apache.org/docs/latest/quick-start.html  | &check; |
| https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark |   &check;   |
| https://spark.apache.org/docs/latest/sql-programming-guide.html | &cross;  |


# Configure Apache Spark on your local machine

- https://telegraphhillsoftware.com/starting-the-spark/
- https://www.knowledgehut.com/blog/big-data/install-spark-on-ubuntu

# How to submit spark job

```
spark-submit --class org.example.arydz.rdd.Main /home/arydz/workspace/learning/Spark-Learning/build/libs/spark-basic-0.1.jar
```
or
```
spark-submit --class org.example.arydz.sql.Main /home/arydz/workspace/learning/Spark-Learning/build/libs/spark-basic-0.1.jar
```

# MongoDB

<span style="color: #9F6000; background-color: #FEEFB3;">**Fill in later**</span><br>
https://www.bmc.com/blogs/mongodb-docker-container/
https://stackoverflow.com/questions/60522471/docker-compose-mongodb-docker-entrypoint-initdb-d-is-not-working
https://medium.com/faun/managing-mongodb-on-docker-with-docker-compose-26bf8a0bbae3

The current js script is only for learning purposes, to force docker compose to init mongo db

# Spark

### RDD - Which Storage Level to Choose?

- If your RDDs fit comfortably with the default storage level (MEMORY_ONLY), leave them that way. This is the most
  CPU-efficient option, allowing operations on the RDDs to run as fast as possible.

- If not, try using MEMORY_ONLY_SER and selecting a fast serialization library to make the objects much more
  space-efficient, but still reasonably fast to access. (Java and Scala)

- Don’t spill to disk unless the functions that computed your datasets are expensive, or they filter a large amount of
  the data. Otherwise, recomputing a partition may be as fast as reading it from disk.

- Use the replicated storage levels if you want fast fault recovery (e.g. if using Spark to serve requests from a web
  application). All the storage levels provide full fault tolerance by recomputing lost data, but the replicated ones
  let you continue running tasks on the RDD without waiting to recompute a lost partition.
