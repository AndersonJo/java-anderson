package ai.incredible.spark;

import org.apache.spark.SparkConf;

public class SparkContextTest {

    public SparkContextTest() {
        SparkConf conf = new SparkConf()
                .setAppName("Spark Conf Test")
                .setMaster("local[*]");
    }
}
