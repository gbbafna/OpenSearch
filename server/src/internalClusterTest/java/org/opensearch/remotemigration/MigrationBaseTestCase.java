/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class MigrationBaseTestCase extends OpenSearchIntegTestCase {
    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;
    boolean addRemote = false;
    Settings extraSettings = Settings.EMPTY;

    private static final int MIN_DOC_COUNT = 50;
    private static final int MAX_DOC_COUNT = 100;

    private final List<String> documentKeys = List.of(
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5)
    );

    void setAddRemote(boolean addRemote) {
        this.addRemote = addRemote;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        setAddRemote(false);
    }

    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        if (addRemote) {
            logger.info("Adding remote store node");
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(extraSettings)
                .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, REPOSITORY_2_NAME, translogRepoPath))
                .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
                .build();
        } else {
            logger.info("Adding docrep node");
            return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
        }
    }

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        /* Adding the following mock plugins:
        - InternalSettingsPlugin : To override default intervals of retention lease and global ckp sync
        - MockFsRepositoryPlugin and MockTransportService.TestPlugin: To ensure remote interactions are not no-op and retention leases are properly propagated
         */
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(
                InternalSettingsPlugin.class,
                MockFsRepositoryPlugin.class,
                MockTransportService.TestPlugin.class,
                MockRepository.Plugin.class
            )
        ).collect(Collectors.toList());
    }

    protected void setFailRate(String repoName, int value) throws ExecutionException, InterruptedException {
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { repoName });
        GetRepositoriesResponse res = client().admin().cluster().getRepositories(gr).get();
        RepositoryMetadata rmd = res.repositories().get(0);
        Settings.Builder settings = Settings.builder()
            .put("location", rmd.settings().get("location"))
            .put("random_data_file_io_exception_rate", value);
        assertAcked(client().admin().cluster().preparePutRepository(repoName).setType("mock").setSettings(settings).get());
    }

    protected void setRegExToFail(String repoName, String value) throws ExecutionException, InterruptedException {
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { repoName });
        GetRepositoriesResponse res = client().admin().cluster().getRepositories(gr).get();
        RepositoryMetadata rmd = res.repositories().get(0);
        Settings.Builder settings = Settings.builder().put("location", rmd.settings().get("location")).put("regexes_to_fail_io", value);
        assertAcked(client().admin().cluster().preparePutRepository(repoName).setType("mock").setSettings(settings).get());
    }

    public static long getOldestFileCreationTime(Path path) throws Exception {
        final AtomicLong oldestFileCreationTime = new AtomicLong(Long.MAX_VALUE);
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException impossible) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.getLastModifiedTime(file).toMillis() < oldestFileCreationTime.get()) {
                    oldestFileCreationTime.set(Files.getLastModifiedTime(file).toMillis());
                }
                return FileVisitResult.CONTINUE;
            }
        });

        return oldestFileCreationTime.get();
    }

    public void initDocRepToRemoteMigration() {
        assertTrue(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
                        .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                )
                .get()
                .isAcknowledged()
        );
    }

    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(60), indices);
    }

    public BulkResponse indexBulk(String indexName, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            final IndexRequest request = client().prepareIndex(indexName)
                .setId(UUIDs.randomBase64UUID())
                .setSource(documentKeys.get(randomIntBetween(0, documentKeys.size() - 1)), randomAlphaOfLength(5))
                .request();
            bulkRequest.add(request);
        }
        return client().bulk(bulkRequest).actionGet();
    }

    protected int numDocs() {
        return between(MIN_DOC_COUNT, MAX_DOC_COUNT);
    }

    Map<String, Integer> getShardCountByNodeId() {
        final Map<String, Integer> shardCountByNodeId = new HashMap<>();
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (final RoutingNode node : clusterState.getRoutingNodes()) {
            logger.info(
                "----> node {} has {} shards",
                node.nodeId(),
                clusterState.getRoutingNodes().node(node.nodeId()).numberOfOwningShards()
            );
            shardCountByNodeId.put(node.nodeId(), clusterState.getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
        }
        return shardCountByNodeId;
    }

    private void indexSingleDoc(String indexName) {
        IndexResponse indexResponse = client().prepareIndex(indexName).setId("id").setSource("field", "value").get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
        DeleteResponse deleteResponse = client().prepareDelete(indexName, "id").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        client().prepareIndex(indexName).setSource("auto", true).get();
    }

    private int bulkIndex(String indexName) throws InterruptedException {
        final int numDocs = numDocs();
        indexBulk(indexName, numDocs);
        return numDocs;
    }

    public class AsyncIndexingService {
        private final String indexName;
        private final AtomicLong indexedDocs = new AtomicLong(0);

        private final AtomicLong singleIndexedDocs = new AtomicLong(0);
        private AtomicBoolean finished = new AtomicBoolean();
        private Thread indexingThread;

        private int refreshFrequency = 3;

        AsyncIndexingService(String indexName) {
            this.indexName = indexName;
        }

        public void startIndexing() {
            indexingThread = getIndexingThread();
            indexingThread.start();
        }

        public void stopIndexing() throws InterruptedException {
            finished.set(true);
            indexingThread.join();
        }

        public long getIndexedDocs() {
            return indexedDocs.get();
        }

        public long getSingleIndexedDocs() {
            return singleIndexedDocs.get();
        }

        private Thread getIndexingThread() {
            return new Thread(() -> {
                int iteration = 0;
                while (finished.get() == false) {
                    long currentDocCount;
                    if (rarely()) {
                        indexSingleDoc(indexName);
                        currentDocCount = indexedDocs.incrementAndGet();
                        singleIndexedDocs.incrementAndGet();
                    } else {
                        try {
                            currentDocCount = indexedDocs.addAndGet(bulkIndex(indexName));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    iteration++;

                    if (currentDocCount > 0 && currentDocCount % refreshFrequency == 0) {
                        if (rarely()) {
                            client().admin().indices().prepareFlush(indexName).get();
                            logger.info("Completed ingestion of {} docs. Flushing now", currentDocCount);
                        } else {
                            client().admin().indices().prepareRefresh(indexName).get();
                        }
                    }
                }
            });
        }

        public void setRefreshFrequency(int refreshFrequency) {
            this.refreshFrequency = refreshFrequency;
        }
    }

    public void excludeNodeSet(String attr, String value) {
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.exclude._" + attr, value))
                .get()
        );
    }

    public void stopShardRebalancing() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none").build())
                .get()
        );
    }

    public ClusterHealthStatus waitForRelocation() {
        ClusterHealthRequest request = Requests.clusterHealthRequest()
            .waitForNoRelocatingShards(true)
            .timeout(TimeValue.timeValueSeconds(60))
            .waitForEvents(Priority.LANGUID);
        ClusterHealthResponse actionGet = client().admin().cluster().health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                "waitForRelocation timed out, cluster state:\n{}\n{}",
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get()
            );
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        return actionGet.getStatus();
    }

    public ClusterHealthStatus waitForRelocation(TimeValue t) {
        ClusterHealthRequest request = Requests.clusterHealthRequest()
            .waitForNoRelocatingShards(true)
            .timeout(t)
            .waitForEvents(Priority.LANGUID);
        ClusterHealthResponse actionGet = client().admin().cluster().health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                "waitForRelocation timed out, cluster state:\n{}\n{}",
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get()
            );
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        return actionGet.getStatus();
    }
}
