import com.couchbase.client.java.Cluster;

public class Runner {
	public static void main(String[] args) {
		CouchbaseConfig config = new CouchbaseConfig("couchbase://localhost", "Administrator", "password");
		Cluster cluster = config.connect();

		CouchbaseTransferService transferService = new CouchbaseTransferService(cluster, "Bucket_1", "Bucket_2", "_default", "_default", "_default", "_default", 1000);
		transferService.transferCollection();
	}
}