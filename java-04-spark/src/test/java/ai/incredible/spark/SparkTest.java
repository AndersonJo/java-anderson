package ai.incredible.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.net.URL;

class SparkTest {
    SparkSession session;

    public SparkTest() {
        session = SparkSession.builder()
                .master("local[*]")
                .appName("Spark Test")
                .config("spark.ui.port", "4050")
                .getOrCreate();
    }

    private URL getResource(){
        return getClass().getClassLoader().getResource("vgsales.csv");
    }

    @Test
    public Dataset<Row> readCSV() {
        String filePath = String.valueOf(getResource());
        Dataset<Row> salesDf = session.read().csv(filePath);
        salesDf.printSchema();
        return salesDf;

    }
}