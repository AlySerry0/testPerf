import com.couchbase.client.java.Cluster;

public class CouchbaseConfig {
	private final String connectionString;
	private final String username;
	private final String password;

	public CouchbaseConfig(String connectionString, String username, String password) {
		this.connectionString = connectionString;
		this.username = username;
		this.password = password;
	}

	public Cluster connect() {
		return Cluster.connect(connectionString, username, password);
	}
}