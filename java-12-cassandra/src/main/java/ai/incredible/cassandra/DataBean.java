package ai.incredible.cassandra;

import lombok.Data;
import lombok.ToString;

import java.sql.Timestamp;

@Data
@ToString
public class DataBean {
	protected String uid;
	protected String name;
	protected Integer age;
	protected Boolean married;
	protected Timestamp createdAt;
	protected Long writetime;
}
