package ai.incredible.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkTest {
    SparkSession spark;

    public SparkTest() {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("Spark Test")
                .config("spark.ui.port", "4050")
                .getOrCreate();
    }

    private URL getResource() {
        return getClass().getClassLoader().getResource("vgsales.csv");
    }

    @Test
    public void testReadCSV() {
        String filePath = String.valueOf(getResource());
        Dataset<Row> salesDf = spark.read()
                .option("header", "true")
                .csv(filePath);

        assertEquals(salesDf.count(), 16598);
        JavaRDD<Row> salesRdd = salesDf.javaRDD();

        salesDf.show(3);
        salesDf.printSchema();

        // Test Platform count / mapToPair
        JavaPairRDD<String, Integer> platformCountPairMap = salesRdd.mapToPair(
                row -> new Tuple2<>(row.getAs("Platform"), 1));
        Map<String, Integer> platformCount = platformCountPairMap
                .reduceByKey((x, y) -> (int) x + (int) y).collectAsMap();
        assertEquals(platformCount.size(), 31);
        assertEquals(platformCount.get("PSP"), 1213);
    }
}