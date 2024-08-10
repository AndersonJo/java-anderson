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
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.column;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.writeTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkTest {
	protected SparkSession spark;

	@BeforeEach
	public void setup() {
		SparkConf conf = new SparkConf()
			.setAppName("Local Spark Example")
			.setMaster("local[2]")
			// .set("spark.cassandra.auth.username", "user_id")
			// .set("spark.cassandra.auth.password", "password")
			// .set("spark.cassandra.input.throughputMBPerSec", "1")
			.set("spark.cassandra.connection.host", "127.0.0.1");

		spark = SparkSession.builder()
			.config(conf)
			.getOrCreate();

		addTestData();
	}

	protected void addTestData() {
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
	}

	@Test
	public void readAllTable() {
		// 방법 1
		// Spark 에서 전체 데이터를 다 가져오기.
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

	@Test
	public void readThroughCassandraConnector1() {
		CassandraTableScanJavaRDD<CassandraRow> rdd =
			javaFunctions(spark.sparkContext())
				.cassandraTable("my_keyspace", "users")
				.select(column("uid"),
					column("name"),
					column("age"),
					column("married"),
					column("created_at").as("createdAt"),
					writeTime("name").as("writetime"));
		JavaRDD<Row> javaRdd = rdd.map(row -> {
			return RowFactory.create(
				row.getString("uid"),
				row.getString("name"),
				row.getInt("age"),
				row.getBoolean("married"),
				new Timestamp(row.getLong("createdAt")),
				row.getLong("writetime"));
		});

		StructType schema = DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("uid", DataTypes.StringType, false),
			DataTypes.createStructField("name", DataTypes.StringType, false),
			DataTypes.createStructField("age", DataTypes.IntegerType, false),
			DataTypes.createStructField("married", DataTypes.BooleanType, false),
			DataTypes.createStructField("createdAt", DataTypes.TimestampType, false),
			DataTypes.createStructField("writetime", DataTypes.LongType, false)
		});

		Dataset<Row> dataset = spark.createDataFrame(javaRdd, schema);
		dataset.show();
		System.out.println(dataset);

	}

	@Test
	public void readThroughCassandraConnector2() {
		// 방법 2
		// Spark Cassandra Connector를 사용해서, 좀더 자세한 정보를 가져오는 방법
		// 회사에서는 됐는데, 지금 여기서는 안됨. select 에서 empty 가 나옴.
		CassandraTableScanJavaRDD<DataBean> rdd = javaFunctions(spark.sparkContext())
			.cassandraTable("my_keyspace", "users", mapRowTo(DataBean.class))
			.select(column("uid"),
				column("name"),
				column("age"),
				column("married"),
				column("created_at").as("createdAt"),
				writeTime("name").as("writetime"));
		JavaRDD<Row> javaRdd = rdd.map(row -> {
			return RowFactory.create(
//				row.getUid(),
//				row.getName(),
//				row.getAge(),
//				row.getMarried(),
//				row.getCreatedAt(),
//				row.getWritetime()
			);
		});
		//
		Dataset<Row> dataset = spark.createDataFrame(javaRdd, DataBean.class);
		dataset.show();
	}

	@Test
	public void cqlSessionTest() {
		// 방법 4
		// CQL 로 direct 접속을 해서 데이터를 가져옵니다.
		// 해당 방법은 spark.read() 를 사용하는 것이 아니며, 이를 spark 에서 사용시에
		// driver 에서 바로 가져오는 것이기 때문에 distributed loading 이 되는 것이 아닙니다.
		// Spark 에서 쓰는 것 보다는 따로 CQL 로 접속해야 할때 사용하면 좋은 방법입니다.
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
		}
	}
}
