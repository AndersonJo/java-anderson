/**
 * Cassandra/bin 들어가서
 * ./cassandra -f
 * 위의 명령어로 카산드라 서버 켜야 함
 */
package ai.incredible.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.*;

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
			// .withAuthCredentials("your_username", "your_password") // 사용자 인증 정보 추가
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

		// 아래 코드는 작동하지 않습니다.
		// WRITETIME 은 오직 CQL에서 제공되며, Spark 에서는 제공되지 않음.
		// 또한 spark.sql 쓰려면 spark.read() 한 이후에 df.createOrReplaceTempView("user_view")
		// 이렇게 만들은 이후에 sql 사용 가능
		// Dataset<Row> data = spark.sql("SELECT *, WRITETIME(age) from my_keyspace.users");

		try (CqlSession session = CqlSession.builder()
			.addContactEndPoint(
				new DefaultEndPoint(new InetSocketAddress("localhost", 9042)))
			.withLocalDatacenter("datacenter1")
			// .withAuthCredentials("your_username", "your_password") // 사용자 인증 정보 추가
			.build()) {

			// 중요한점! ALLOW FILTERING 에 끝에 들어갔음.
			// Cassandra 에서는 WHERE statement 가 연산량이 많은듯 함.
			// 그래서 WHERE 사용시 반드시 뒤에 ALLOW FILTERING 써줘야 함
			// 또한 setPageSize 를 통해서 한번에 얼마나 가져올지를 정함
			String query = "SELECT name, age, WRITETIME(name) as created_at "
				+ "FROM my_keyspace.users WHERE name='Anderson' ALLOW FILTERING;";
			ResultSet resultSet = session.execute(SimpleStatement.builder(query)
				.setPageSize(5).build());

			List<Row> rows = new ArrayList<>();
			do {
				for (com.datastax.oss.driver.api.core.cql.Row cassandraRow : resultSet) {
					rows.add(RowFactory.create(
						cassandraRow.getString("name"),
						cassandraRow.getInt("age"),
						new Timestamp(cassandraRow.getLong("created_at") / 1000)
					));
				}

			} while (!resultSet.isFullyFetched());

			StructType schema2 = DataTypes.createStructType(new StructField[] {
				DataTypes.createStructField("name", DataTypes.StringType, false),
				DataTypes.createStructField("age", DataTypes.IntegerType, false),
				DataTypes.createStructField("created_at", DataTypes.TimestampType, false)
			});

			Dataset<Row> df2 = spark.createDataFrame(rows, schema2);
			df2.show();

			assertTrue(df2.count() >= 3);
			andersonRow = df2.filter("name = 'Anderson'").first();
			assertEquals(40, (int) andersonRow.getAs("age"));
			assertEquals(2024, andersonRow.getTimestamp(2).toLocalDateTime().getYear());
		}
	}
}
