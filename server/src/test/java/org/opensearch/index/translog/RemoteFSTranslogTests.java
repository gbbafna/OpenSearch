/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;
import static org.opensearch.index.translog.SnapshotMatchers.containsOperationsInAnyOrder;
import static org.opensearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class RemoteFSTranslogTests extends OpenSearchTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    protected RemoteFsTranslog translog;
    private AtomicLong globalCheckpoint;
    protected Path translogDir;
    // A default primary term is used by translog instances created in this test.
    private final AtomicLong primaryTerm = new AtomicLong();
    private final AtomicReference<LongConsumer> persistedSeqNoConsumer = new AtomicReference<>();
    private boolean expectIntactTranslog;
    private ThreadPool threadPool;

    BlobStoreRepository repository;

    BlobStoreTransferService blobStoreTransferService;

    private LongConsumer getPersistedSeqNoConsumer() {
        return seqNo -> {
            final LongConsumer consumer = persistedSeqNoConsumer.get();
            if (consumer != null) {
                consumer.accept(seqNo);
            }
        };
    }


    protected Translog createTranslog(TranslogConfig config) throws IOException {
        String translogUUID = Translog.createEmptyTranslog(
            config.getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            primaryTerm.get()
        );
        return new LocalTranslog(
            config,
            translogUUID,
            createTranslogDeletionPolicy(config.getIndexSettings()),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            primaryTerm::get,
            getPersistedSeqNoConsumer()
        );
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm.set(randomLongBetween(1, Integer.MAX_VALUE));
        // if a previous test failed we clean up things here
        translogDir = createTempDir();
        translog = create(translogDir);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            translog.getDeletionPolicy().assertNoOpenTranslogRefs();
            translog.close();
        } finally {
            super.tearDown();
            terminate(threadPool);
        }
    }

    private RemoteFsTranslog create(Path path) throws IOException {
        globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final TranslogConfig translogConfig = getTranslogConfig(path);
        final TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy(translogConfig.getIndexSettings());
        final String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        repository = createRepository();
        threadPool = new TestThreadPool(getClass().getName());
        blobStoreTransferService =  new BlobStoreTransferService(repository.blobStore(), threadPool);
        return new RemoteFsTranslog(
            translogConfig,
            translogUUID,
            deletionPolicy,
            () -> globalCheckpoint.get(),
            primaryTerm::get,
            getPersistedSeqNoConsumer(),
            repository,
            threadPool
        );

    }

    private TranslogConfig getTranslogConfig(final Path path) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            // only randomize between nog age retention and a long one, so failures will have a chance of reproducing
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomBoolean() ? "-1ms" : "1h")
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), randomIntBetween(-1, 2048) + "b")
            .build();
        return getTranslogConfig(path, settings);
    }

    private TranslogConfig getTranslogConfig(final Path path, final Settings settings) {
        final ByteSizeValue bufferSize = randomFrom(
            TranslogConfig.DEFAULT_BUFFER_SIZE,
            new ByteSizeValue(8, ByteSizeUnit.KB),
            new ByteSizeValue(10 + randomInt(128 * 1024), ByteSizeUnit.BYTES)
        );

        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings);
        return new TranslogConfig(shardId, path, indexSettings, NON_RECYCLING_INSTANCE, bufferSize);
    }

    private BlobStoreRepository createRepository() {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);
        final FsRepository repository = new FsRepository(
            repositoryMetadata,
            createEnvironment(),
            xContentRegistry(),
            clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        ) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually
            }
        };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        repository.start();
        return repository;
    }


    /** Create a {@link Environment} with random path.home and path.repo **/
    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath())
                .build()
        );
    }

    private Translog.Location addToTranslogAndList(Translog translog, List<Translog.Operation> list, Translog.Operation op) throws IOException {
        Translog.Location loc = translog.add(op);
        Random random = random();
        if (random.nextBoolean()) {
            translog.ensureSynced(loc);
        }
        list.add(op);
        return loc;
    }

    private Translog.Location addToTranslogAndListAndUpload(Translog translog, List<Translog.Operation> list, Translog.Operation op) throws IOException {
        Translog.Location loc = translog.add(op);
        translog.ensureSynced(loc);
        list.add(op);
        return loc;
    }

    public void testSimpleOperations() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.size(0));
        }

        addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
        }

        addToTranslogAndList(translog, ops, new Translog.Delete("2", 1, primaryTerm.get()));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
            assertThat(snapshot, containsOperationsInAnyOrder(ops));
        }

        final long seqNo = randomLongBetween(0, Integer.MAX_VALUE);
        final String reason = randomAlphaOfLength(16);
        final long noopTerm = randomLongBetween(1, primaryTerm.get());
        addToTranslogAndList(translog, ops, new Translog.NoOp(seqNo, noopTerm, reason));

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            Translog.Index index = (Translog.Index) snapshot.next();
            assertNotNull(index);
            assertThat(BytesReference.toBytes(index.source()), equalTo(new byte[] { 1 }));

            Translog.Delete delete = (Translog.Delete) snapshot.next();
            assertNotNull(delete);
            assertThat(delete.id(), equalTo("2"));


            Translog.NoOp noOp = (Translog.NoOp) snapshot.next();
            assertNotNull(noOp);
            assertThat(noOp.seqNo(), equalTo(seqNo));
            assertThat(noOp.primaryTerm(), equalTo(noopTerm));
            assertThat(noOp.reason(), equalTo(reason));

            assertNull(snapshot.next());
        }

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, containsOperationsInAnyOrder(ops));
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
        }

        try (Translog.Snapshot snapshot = translog.newSnapshot(seqNo + 1, randomLongBetween(seqNo + 1, Long.MAX_VALUE))) {
            assertThat(snapshot, SnapshotMatchers.size(0));
            assertThat(snapshot.totalOperations(), equalTo(0));
        }

    }

    public void testReadLocation() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        ArrayList<Translog.Location> locs = new ArrayList<>();
        locs.add(addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 })));
        locs.add(addToTranslogAndList(translog, ops, new Translog.Index("2", 1, primaryTerm.get(), new byte[] { 1 })));
        locs.add(addToTranslogAndList(translog, ops, new Translog.Index("3", 2, primaryTerm.get(), new byte[] { 1 })));
        translog.sync();
        int i = 0;
        for (Translog.Operation op : ops) {
            assertEquals(op, translog.readOperation(locs.get(i++)));
        }
        assertNull(translog.readOperation(new Translog.Location(100, 0, 0)));
    }

    public void testSnapshotWithNewTranslog() throws IOException {
        List<Closeable> toClose = new ArrayList<>();
        try {
            ArrayList<Translog.Operation> ops = new ArrayList<>();
            Translog.Snapshot snapshot = translog.newSnapshot();
            toClose.add(snapshot);
            assertThat(snapshot, SnapshotMatchers.size(0));

            addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
            Translog.Snapshot snapshot1 = translog.newSnapshot();
            toClose.add(snapshot1);

            addToTranslogAndList(translog, ops, new Translog.Index("2", 1, primaryTerm.get(), new byte[] { 2 }));

            assertThat(snapshot1, SnapshotMatchers.equalsTo(ops.get(0)));

            translog.rollGeneration();
            addToTranslogAndList(translog, ops, new Translog.Index("3", 2, primaryTerm.get(), new byte[] { 3 }));

            Translog.Snapshot snapshot2 = translog.newSnapshot();
            toClose.add(snapshot2);
            translog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(2);
            assertThat(snapshot2, containsOperationsInAnyOrder(ops));
            assertThat(snapshot2.totalOperations(), equalTo(ops.size()));
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    public void testSnapshotOnClosedTranslog() throws IOException {
        assertTrue(Files.exists(translogDir.resolve(Translog.getFilename(1))));
        translog.add(new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
        translog.close();
        AlreadyClosedException ex = expectThrows(AlreadyClosedException.class, () -> translog.newSnapshot());
        assertEquals(ex.getMessage(), "translog is already closed");
    }

    public void testRangeSnapshot() throws Exception {
        long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final int generations = between(2, 20);
        Map<Long, List<Translog.Operation>> operationsByGen = new HashMap<>();
        for (int gen = 0; gen < generations; gen++) {
            Set<Long> seqNos = new HashSet<>();
            int numOps = randomIntBetween(1, 100);
            for (int i = 0; i < numOps; i++) {
                final long seqNo = randomValueOtherThanMany(seqNos::contains, () -> randomLongBetween(0, 1000));
                minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
                maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
                seqNos.add(seqNo);
            }
            List<Translog.Operation> ops = new ArrayList<>(seqNos.size());
            for (long seqNo : seqNos) {
                Translog.Index op = new Translog.Index(randomAlphaOfLength(10), seqNo, primaryTerm.get(), new byte[] { randomByte() });
                translog.add(op);
                ops.add(op);
            }
            operationsByGen.put(translog.currentFileGeneration(), ops);
            translog.rollGeneration();
            if (rarely()) {
                translog.rollGeneration(); // empty generation
            }
        }

        if (minSeqNo > 0) {
            long fromSeqNo = randomLongBetween(0, minSeqNo - 1);
            long toSeqNo = randomLongBetween(fromSeqNo, minSeqNo - 1);
            try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo)) {
                assertThat(snapshot.totalOperations(), equalTo(0));
                assertNull(snapshot.next());
            }
        }

        long fromSeqNo = randomLongBetween(maxSeqNo + 1, Long.MAX_VALUE);
        long toSeqNo = randomLongBetween(fromSeqNo, Long.MAX_VALUE);
        try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo)) {
            assertThat(snapshot.totalOperations(), equalTo(0));
            assertNull(snapshot.next());
        }

        fromSeqNo = randomLongBetween(0, 2000);
        toSeqNo = randomLongBetween(fromSeqNo, 2000);
        try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo)) {
            Set<Long> seenSeqNos = new HashSet<>();
            List<Translog.Operation> expectedOps = new ArrayList<>();
            for (long gen = translog.currentFileGeneration(); gen > 0; gen--) {
                for (Translog.Operation op : operationsByGen.getOrDefault(gen, Collections.emptyList())) {
                    if (fromSeqNo <= op.seqNo() && op.seqNo() <= toSeqNo && seenSeqNos.add(op.seqNo())) {
                        expectedOps.add(op);
                    }
                }
            }
            assertThat(TestTranslog.drainSnapshot(snapshot, false), equalTo(expectedOps));
        }
    }

    public void testSimpleOperationsUpload() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.size(0));
        }

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
        }

        assertEquals(translog.allUploaded().size(), 4);

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 1, primaryTerm.get(), new byte[] { 1 }));
        assertEquals(translog.allUploaded().size(), 6);

        translog.rollGeneration();
        assertEquals(translog.allUploaded().size(), 6);

        Set<String> mdFiles = blobStoreTransferService.listAll(repository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())).add("metadata"));
        assertEquals(mdFiles.size(), 2);
        logger.info("All md files {}", mdFiles);

        Set<String> tlogFiles = blobStoreTransferService.listAll(repository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())).add(String.valueOf(primaryTerm.get())));
        logger.info("All data files {}", tlogFiles);

        // assert content of ckp and tlog files
        BlobPath path = repository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())).add(String.valueOf(primaryTerm.get()));
        BlobPath mdPath = repository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())).add("metadata");
        for (TranslogReader reader : translog.readers) {
            final long readerGeneration = reader.getGeneration();
            logger.error("Asserting content of {}", readerGeneration);
            Path translogPath = reader.path();
            try (CheckedInputStream stream = new CheckedInputStream(new FileInputStream(translogPath.toFile()), new CRC32())) {
                byte[] content = stream.readAllBytes();
                byte[] tlog = blobStoreTransferService.readFile(path, Translog.getFilename(readerGeneration)).readAllBytes();
                assertArrayEquals(tlog, content);
            }

            Path checkpointPath = translog.location().resolve(Translog.getCommitCheckpointFileName(readerGeneration));
            try (CheckedInputStream stream = new CheckedInputStream(new FileInputStream(checkpointPath.toFile()), new CRC32())) {
                byte[] content = stream.readAllBytes();
                byte[] ckp = blobStoreTransferService.readFile(mdPath, Translog.getCommitCheckpointFileName(readerGeneration)).readAllBytes();
                assertArrayEquals(ckp, content);
            }
        }
    }

}
