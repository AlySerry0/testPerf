import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CouchbaseTransferService {
	private static final Logger logger = LoggerFactory.getLogger(CouchbaseTransferService.class);

	private final AsyncCluster asyncCluster;
	private final String sourceBucketName;
	private final String targetBucketName;
	private final String sourceScopeName;
	private final String sourceCollectionName;
	private final String targetScopeName;
	private final String targetCollectionName;
	private final int batchSize;
	private final ExecutorService executorService;

	public CouchbaseTransferService(Cluster cluster, String sourceBucketName, String targetBucketName, String sourceScopeName, String sourceCollectionName, String targetScopeName, String targetCollectionName, int batchSize) {
		this.asyncCluster = cluster.async();
		this.sourceBucketName = sourceBucketName;
		this.targetBucketName = targetBucketName;
		this.sourceScopeName = sourceScopeName;
		this.sourceCollectionName = sourceCollectionName;
		this.targetScopeName = targetScopeName;
		this.targetCollectionName = targetCollectionName;
		this.batchSize = batchSize;
		this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}

	public int countTotalDocuments() {
		String countQuery = String.format("SELECT COUNT(*) AS total FROM `%s`.`%s`.`%s`", sourceBucketName, sourceScopeName, sourceCollectionName);
		QueryResult result = asyncCluster.query(countQuery).join();
		return result.rowsAsObject().get(0).getInt("total");
	}

	public void transferBatch(int startOffset) {
		AsyncBucket sourceBucket = asyncCluster.bucket(sourceBucketName);
		AsyncBucket targetBucket = asyncCluster.bucket(targetBucketName);
		AsyncScope sourceScope = sourceBucket.scope(sourceScopeName);
		AsyncScope targetScope = targetBucket.scope(targetScopeName);
		AsyncCollection sourceCollection = sourceScope.collection(sourceCollectionName);
		AsyncCollection targetCollection = targetScope.collection(targetCollectionName);

		String query = String.format("SELECT META().id FROM `%s`.`%s`.`%s` LIMIT %d OFFSET $offset", sourceBucketName, sourceScopeName, sourceCollectionName, batchSize);
		AtomicInteger offset = new AtomicInteger(startOffset);
		AtomicInteger documentCounter = new AtomicInteger();
		AtomicLong startTime = new AtomicLong(System.currentTimeMillis());

		List<CompletableFuture<Void>> allFutures = new ArrayList<>();

		while (documentCounter.get() < batchSize) {
			CompletableFuture<QueryResult> queryFuture = asyncCluster.query(query, QueryOptions.queryOptions().parameters(JsonObject.create().put("offset", offset.get())).scanConsistency(QueryScanConsistency.REQUEST_PLUS));
			QueryResult result = queryFuture.join();

			List<JsonObject> rows = result.rowsAsObject();
			if (rows.isEmpty()) {
				break;
			}

			List<CompletableFuture<Void>> transferFutures = new ArrayList<>();
			List<String> successfulResults = Collections.synchronizedList(new ArrayList<>());
			Map<String, Throwable> erroredResults = new ConcurrentHashMap<>();

			for (int i = 0; i < rows.size(); i += batchSize) {
				List<JsonObject> batch = rows.subList(i, Math.min(i + batchSize, rows.size()));
				CompletableFuture<Void> batchFuture = CompletableFuture.allOf(batch.stream().map(row -> {
					String id = row.getString("id");
					return sourceCollection.get(id).thenCompose(doc -> targetCollection.upsert(id, doc.contentAsObject())).thenAccept(success -> {
						int count = documentCounter.incrementAndGet();
						synchronized (logger) {
							if (count % 10000 == 0) {
								logger.info("Transferred {} documents so far", count);
							}
							if (count % 100000 == 0) {
								long endTime = System.currentTimeMillis();
								logger.info("Time taken to transfer 100,000 documents: {} ms", (endTime - startTime.get()));
								startTime.set(endTime);
							}
						}
						successfulResults.add(id);
					}).exceptionally(error -> {
						erroredResults.put(id, error);
						return null;
					});
				}).toArray(CompletableFuture[]::new));
				transferFutures.add(batchFuture);
			}

			CompletableFuture<Void> allBatchesFuture = CompletableFuture.allOf(transferFutures.toArray(new CompletableFuture[0]));
			allFutures.add(allBatchesFuture);

			offset.addAndGet(batchSize);
		}

		CompletableFuture<Void> allTransfersFuture = CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]));
		allTransfersFuture.join();
		logger.info("Batch transfer completed. Total documents transferred in this batch: {}", documentCounter.get());

		// Ensure all threads are terminated
		asyncCluster.disconnect().join();
		executorService.shutdown();
	}
}