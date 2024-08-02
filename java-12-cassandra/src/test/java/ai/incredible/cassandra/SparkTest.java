package ai.incredible.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkTest {

	@Test
	public void sparkTest() {
		SparkConf conf = new SparkConf()
			.setAppName("Local Spark Example")
			.setMaster("local[2]")
			.set("spark.cassandra.connection.host", "127.0.0.1");

		SparkSession spark = SparkSession.builder()
			.config(conf)
			.getOrCreate();

		try (CqlSession session = CqlSession.builder()
			.addContactEndPoint(
				new DefaultEndPoint(new InetSocketAddress("localhost", 9042)))
			.withLocalDatacenter("datacenter1")
			// .withAuthCredentials()
			.build()) {
			String createKeySpace = "CREATE KEYSPACE IF NOT EXISTS my_keyspace "
				+ "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";

			String createTable =
				"CREATE TABLE IF NOT EXISTS my_keyspace.users ("
					+ "uid UUID PRIMARY KEY, "
					+ "name text, "
					+ "age int, "
					+ "married boolean,"
					+ "created_at timestamp);";

			System.out.println(createTable);
			session.execute(createKeySpace);
			session.execute(createTable);

		}

		// 데이터 생성
		StructType schema = DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("uid", DataTypes.StringType, false),
			DataTypes.createStructField("name", DataTypes.StringType, false),
			DataTypes.createStructField("age", DataTypes.IntegerType, false),
			DataTypes.createStructField("married", DataTypes.BooleanType, false),
			DataTypes.createStructField("created_at", DataTypes.TimestampType, false)
		});

		Timestamp timestamp = new Timestamp(new Date().getTime());
		Dataset<Row> userData = spark.createDataFrame(Arrays.asList(
			RowFactory.create(UUID.randomUUID().toString(), "Anderson", 40, true, timestamp),
			RowFactory.create(UUID.randomUUID().toString(), "Alice", 25, false, timestamp),
			RowFactory.create(UUID.randomUUID().toString(), "Yoona", 21, false, timestamp)
		), schema);

		userData.write()
			.format("org.apache.spark.sql.cassandra")
			.mode(SaveMode.Append)
			.option("keyspace", "my_keyspace")
			.option("table", "users")
			.save();

		// 데이터 가져오기
		Dataset<Row> df = spark.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "my_keyspace")
			.option("table", "users")
			.load();

		assertTrue(df.count() >= 3);
		Row andersonRow = df.filter("name = 'Anderson'").first();
		assertEquals(40, (int) andersonRow.getAs("age"));
		assertEquals(true, andersonRow.getAs("married"));

		df.show();
	}
}
