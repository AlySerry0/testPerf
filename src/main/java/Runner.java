import com.couchbase.client.java.Cluster;

public class Runner {
	public static void main(String[] args) {
		System.setProperty("java.awt.headless", "true");
		System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");

		CouchbaseConfig config = new CouchbaseConfig("couchbase://localhost", "Administrator", "password");
		Cluster cluster = config.connect();

		int batchSize = 100000;
		int totalDocuments = new CouchbaseTransferService(cluster, "Bucket_1", "Bucket_2", "_default", "_default", "_default", "_default", batchSize).countTotalDocuments();
		int iterations = (int) Math.ceil((double) totalDocuments / batchSize);

		for (int i = 0; i < iterations; i++) {
			CouchbaseTransferService transferService = new CouchbaseTransferService(cluster, "Bucket_1", "Bucket_2", "_default", "_default", "_default", "_default", batchSize);
			transferService.transferBatch(i * batchSize);
		}
	}
}