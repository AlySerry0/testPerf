import com.couchbase.client.java.Cluster;

public class Runner {
	public static void main(String[] args) {
		System.setProperty("java.awt.headless", "true");
		System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");

		CouchbaseConfig config = new CouchbaseConfig("couchbase://localhost", "Administrator", "password");
		Cluster cluster = config.connect();

		CouchbaseTransferService transferService = new CouchbaseTransferService(cluster, "Bucket_1", "Bucket_2", "_default", "_default", "_default", "_default", 1000);
		transferService.transferCollection();
	}
}