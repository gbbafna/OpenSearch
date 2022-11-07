/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TransferSnapshotProvider;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class RemoteFsTranslog extends Translog {

    private final BlobStoreRepository blobStoreRepository;
    private final TranslogTransferManager translogTransferManager;
    private final static String METADATA_DIR = "metadata";

    public RemoteFsTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool
    ) throws IOException {
        super(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer);
        this.blobStoreRepository = blobStoreRepository;
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId);
        this.translogTransferManager = new TranslogTransferManager(
            new BlobStoreTransferService(blobStoreRepository.blobStore(), threadPool),
            blobStoreRepository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())),
            blobStoreRepository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())).add(METADATA_DIR),
            fileTransferTracker,
            fileTransferTracker::exclusionFilter
        );
        try {
            // Copy all  remote translog files to location
            final Checkpoint checkpoint = readCheckpoint(location);
            this.readers.addAll(recoverFromFiles(checkpoint));
            if (readers.isEmpty()) {
                throw new IllegalStateException("at least one reader must be recovered");
            }

            boolean success = false;
            current = null;
            try {
                current = createWriter(
                    checkpoint.generation + 1,
                    getMinFileGeneration(),
                    checkpoint.globalCheckpoint,
                    persistedSequenceNumberConsumer
                );
                success = true;
            } finally {
                // we have to close all the recovered ones otherwise we leak file handles here
                // for instance if we have a lot of tlog and we can't create the writer we keep
                // on holding
                // on to all the uncommitted tlog files if we don't close
                if (success == false) {
                    IOUtils.closeWhileHandlingException(readers);
                }
            }
        } catch (Exception e) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    public RemoteFsTranslog(
        RemoteTranslogMetadata remoteTranslogMetadata,
        RemoteFsTranslog old
    ) throws IOException {
        super(old.config, old.translogUUID, old.deletionPolicy, old.globalCheckpointSupplier, old.primaryTermSupplier, old.persistedSequenceNumberConsumer);
        this.blobStoreRepository = old.blobStoreRepository;
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId);
        this.translogTransferManager = new TranslogTransferManager(
            new BlobStoreTransferService(blobStoreRepository.blobStore(), old.blobStoreRepository.threadPool()),
            blobStoreRepository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())),
            blobStoreRepository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())).add(METADATA_DIR),
            fileTransferTracker,
            fileTransferTracker::exclusionFilter
        );
        try {
            boolean success = false;
            // Copy all  remote translog files to location
            // which location for now?... same location

            // Remove files not present
        } finally {

        }


    }

    /** recover all translog files found on disk */
    protected ArrayList<TranslogReader> recoverFromFiles(Checkpoint checkpoint) throws IOException {
        boolean success = false;
        ArrayList<TranslogReader> foundTranslogs = new ArrayList<>();
        try (ReleasableLock ignored = writeLock.acquire()) {
            logger.debug("open uncommitted translog checkpoint {}", checkpoint);
            final long minGenerationToRecoverFrom = checkpoint.minTranslogGeneration;

            // we open files in reverse order in order to validate the translog uuid before we start traversing the translog based on
            // the generation id we found in the lucene commit. This gives for better error messages if the wrong
            // translog was found.
            for (long i = checkpoint.generation; i >= minGenerationToRecoverFrom; i--) {
                Path committedTranslogFile = location.resolve(Translog.getFilename(i));
                if (Files.exists(committedTranslogFile) == false) {
                    throw new TranslogCorruptedException(
                        committedTranslogFile.toString(),
                        "translog file doesn't exist with generation: "
                            + i
                            + " recovering from: "
                            + minGenerationToRecoverFrom
                            + " checkpoint: "
                            + checkpoint.generation
                            + " - translog ids must be consecutive"
                    );
                }
                final Checkpoint readerCheckpoint = i == checkpoint.generation
                    ? checkpoint
                    : Checkpoint.read(location.resolve(Translog.getCommitCheckpointFileName(i)));
                final TranslogReader reader = openReader(committedTranslogFile, readerCheckpoint);
                assert reader.getPrimaryTerm() <= primaryTermSupplier.getAsLong() : "Primary terms go backwards; current term ["
                    + primaryTermSupplier.getAsLong()
                    + "] translog path [ "
                    + committedTranslogFile
                    + ", existing term ["
                    + reader.getPrimaryTerm()
                    + "]";
                foundTranslogs.add(reader);
                logger.debug("recovered local translog from checkpoint {}", checkpoint);
            }
            Collections.reverse(foundTranslogs);

            // when we clean up files, we first update the checkpoint with a new minReferencedTranslog and then delete them;
            // if we crash just at the wrong moment, it may be that we leave one unreferenced file behind so we delete it if there
            IOUtils.deleteFilesIgnoringExceptions(
                location.resolve(Translog.getFilename(minGenerationToRecoverFrom - 1)),
                location.resolve(Translog.getCommitCheckpointFileName(minGenerationToRecoverFrom - 1))
            );

            Path commitCheckpoint = location.resolve(Translog.getCommitCheckpointFileName(checkpoint.generation));
            if (Files.exists(commitCheckpoint)) {
                Checkpoint checkpointFromDisk = Checkpoint.read(commitCheckpoint);
                if (checkpoint.equals(checkpointFromDisk) == false) {
                    throw new TranslogCorruptedException(
                        commitCheckpoint.toString(),
                        "checkpoint file "
                            + commitCheckpoint.getFileName()
                            + " already exists but has corrupted content: expected "
                            + checkpoint
                            + " but got "
                            + checkpointFromDisk
                    );
                }
            } else {
                copyCheckpointTo(commitCheckpoint);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(foundTranslogs);
            }
        }
        return foundTranslogs;
    }

    @Override
    boolean ensureSynced(Location location) throws IOException {
        Callable<Boolean> execute = () -> false;
        try (ReleasableLock lock = readLock.acquire()) {
            assert location.generation <= current.getGeneration();
            if (location.generation == current.getGeneration()) {
                ensureOpen();
                execute = () -> prepareAndUpload(primaryTermSupplier.getAsLong(), location.generation);
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        try {
            return execute.call();
        } catch (Exception ex) {
            closeOnTragicEvent(ex);
            assert ex instanceof IOException;
            throw (IOException) ex;
        }
    }

    @Override
    public void rollGeneration() throws IOException {
        syncBeforeRollGeneration();
        if (current.totalOperations() == 0 && primaryTermSupplier.getAsLong() == current.getPrimaryTerm()) {
            return;
        }
        prepareAndUpload(primaryTermSupplier.getAsLong(), null);
    }

    private boolean prepareAndUpload(Long primaryTerm, Long generation) throws IOException {
        try (Releasable ignored = writeLock.acquire()) {
            if (generation == null || generation == current.getGeneration()) {
                try {
                    final TranslogReader reader = current.closeIntoReader();
                    readers.add(reader);
                    copyCheckpointTo(location.resolve(getCommitCheckpointFileName(current.getGeneration())));
                    if (closed.get() == false) {
                        logger.trace("Creating new writer for gen: [{}]", current.getGeneration() + 1);
                        current = createWriter(current.getGeneration() + 1);
                        logger.trace("current translog set to [{}]", current.getGeneration());
                        if (generation == null ) {
                            // ToDo : Verify the impact of not updating global checkpoint on remote txlog.
                            logger.info("gbbafna - For checkpointing setting gen to {} ", current.getGeneration());
                            return true;
                        }
                    }
                } catch (final Exception e) {
                    tragedy.setTragicException(e);
                    closeOnTragicEvent(e);
                    throw e;
                }
            }
        }
        return upload(primaryTerm, generation);
    }

    private boolean upload(Long primaryTerm, Long generation) throws IOException {
        logger.info("gbbafna uploading txlog for {} {} ", primaryTerm, generation);
        TransferSnapshotProvider transferSnapshotProvider = new TransferSnapshotProvider(primaryTerm, generation, this.location, readers);
        Releasable transferReleasable = Releasables.wrap(deletionPolicy.acquireTranslogGen(getMinFileGeneration()));
        return translogTransferManager.uploadTranslog(transferSnapshotProvider.get(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) throws IOException {
                transferReleasable.close();
                closeFilesIfNoPendingRetentionLocks();
                updateReaders();
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) throws IOException {
                transferReleasable.close();
                closeFilesIfNoPendingRetentionLocks();
            }
        });
    }

    private boolean syncToDisk() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.sync();
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    public boolean updateReaders() throws IOException {
        //recover all translog files found on remote translog
        RemoteTranslogMetadata remoteTranslogMetadata = translogTransferManager.findLatestMetadata();
        Map<String, Object>  generationToPrimaryTermMapper =  remoteTranslogMetadata.getGenerationToPrimaryTermMapper();

        Set<Long> generations = new HashSet<>();
        generationToPrimaryTermMapper.entrySet().stream()
            .forEach(
                genToTerm -> {
                    generations.add(Long.parseLong(genToTerm.getKey(), Character.MAX_RADIX));
                }
            );

        long maxGen = Collections.max(generations);
        long maxPrimary = Long.parseLong((String) generationToPrimaryTermMapper.get(Long.toString(maxGen, Character.MAX_RADIX)));
        logger.info("Highest gen and primary {} {}", maxGen, maxPrimary);
        Tuple<byte[], byte[]> t2 =  translogTransferManager.readTranslogGen(maxPrimary, maxGen);
        Path file2 = location.resolve(getCommitCheckpointFileName(maxGen));
        Files.deleteIfExists(file2);
        final FileChannel channel2 = getChannelFactory().open(file2);
        channel2.write(ByteBuffer.wrap(t2.v2()));
        channel2.force(true);
        final Checkpoint checkpoint = Checkpoint.read(file2);
        logger.info("updated checkpoint {} {}", checkpoint);

        generationToPrimaryTermMapper.entrySet().stream()
        .forEach(
            genToTerm -> {
                Long gen = Long.parseLong(genToTerm.getKey(), Character.MAX_RADIX);
                Long primary = Long.parseLong((String) genToTerm.getValue(), Character.MAX_RADIX);

                try {
                    Tuple<byte[], byte[]> t =  translogTransferManager.readTranslogGen(primary, gen);
                    Path file = location.resolve(getFilename(gen));
                    Files.deleteIfExists(file);
                    final FileChannel channel = getChannelFactory().open(file);
                    channel.write(ByteBuffer.wrap(t.v1()));
                    channel.force(true);

                    file = location.resolve(getCommitCheckpointFileName(gen));
                    Files.deleteIfExists(file);
                    final FileChannel ckpChannel = getChannelFactory().open(file);
                    ckpChannel.write(ByteBuffer.wrap(t.v2()));
                    ckpChannel.force(true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        );
        reloadReaders(checkpoint);

        return true;
    }

    public void reloadReaders(Checkpoint checkpoint) throws IOException {
        int oldSize = this.readers.size();

        logger.info("reloading readers {}", this.readers.size());
        ArrayList<Long> oldGens = new ArrayList<>();
        ArrayList<Long> newGens = new ArrayList<>();
        Iterator<TranslogReader> iterator = this.readers.iterator();
        TranslogReader reader;
        while (iterator.hasNext()) {
            reader = iterator.next();
            oldGens.add(reader.getCheckpoint().generation);
        }

        this.readers.clear();
        this.readers.addAll(recoverFromFiles(checkpoint));

        iterator = this.readers.iterator();
        while (iterator.hasNext()) {
            reader = iterator.next();
            newGens.add(reader.getCheckpoint().generation);
        }

        assert(oldGens.equals(newGens));
        assert(this.readers.size() == oldSize);

        logger.info("reloaded readers {}", this.readers.size());
    }

    @Override
    public void sync() throws IOException {

        try {
            if (syncToDisk()) {
                prepareAndUpload(primaryTermSupplier.getAsLong(), null);
            }
        } catch (final Exception e) {
            tragedy.setTragicException(e);
            closeOnTragicEvent(e);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        assert Translog.calledFromOutsideOrViaTragedyClose()
            : "Translog.close method is called from inside Translog, but not via closeOnTragicEvent method";
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                prepareAndUpload(primaryTermSupplier.getAsLong(), null);
            } finally {
                logger.debug("translog closed");
            }
        }
    }
}
