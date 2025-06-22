package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Implementation for collecting warm metrics at per query level
 */
public class WarmPerQueryMetricImpl implements WarmPerQueryMetric {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(
        WarmPerQueryMetricImpl.class);
    private static final long FULL_FILE_BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RemoteStoreStatFullFile.class);
    private static final long FC_BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FileCacheStat.class);
    private static final Logger logger = LogManager.getLogger(WarmPerQueryMetricImpl.class);


    // File Cache stats will include hit/miss for both block and full file
    protected final Map<String, FileCacheStat> fileCacheStats;
    protected final Map<String, RemoteStoreStat> remoteStoreStats;
    protected final Map<String, Long> prefetchFiles;
    protected final Map<String, Long> readAheadFiles;
    protected long effectiveBytes;
    protected List<long[]> downloadIntervals;
    protected long hits;
    protected long miss;
    private final String parentTaskId;
    private final String shardId;
    private static final long BYTES_IN_MB = 1024 * 1024;
    private final long startTime;
    private long endTime;

    protected final Map<String, FileCacheStat> fullFileCacheStats;
    protected final Map<String, RemoteStoreStatFullFile> fullFileS3ServiceStats;
    protected long fullFileEffectiveBytes;
    protected long fullFileElapsedTimeNanos;
    protected long fullFileHits;
    protected long fullFileMiss;

    public WarmPerQueryMetricImpl(String parentTaskId, String shardId) {
        this.parentTaskId = parentTaskId;
        this.shardId = shardId;
        this.fileCacheStats = new HashMap<>();
        this.remoteStoreStats = new HashMap<>();
        this.prefetchFiles = new HashMap<>();
        this.readAheadFiles = new HashMap<>();
        this.downloadIntervals = new ArrayList<>();
        this.effectiveBytes = 0L;
        this.hits = 0L;
        this.miss = 0L;
        this.startTime = System.currentTimeMillis();
        this.endTime = 0L;

        this.fullFileCacheStats = new HashMap<>();
        this.fullFileS3ServiceStats = new HashMap<>();
        this.fullFileEffectiveBytes = 0L;
        this.fullFileElapsedTimeNanos = 0L;
        this.fullFileHits = 0L;
        this.fullFileMiss = 0L;
    }

    public FileBlock getFileBlock(String blockFileName) {
        String[] fileParts = blockFileName.split("/", -1);
        fileParts = fileParts[fileParts.length - 1].split("_block_");

        if (fileParts.length == 2) {
            //SlowLog ToDo - fix me .
            return new FileBlock(fileParts[0], Integer.parseInt(fileParts[1]));
        } else {
            assert false : "getFileBlock called with invalid block name, possibly without the extension";
            return new FileBlock(blockFileName, -1);
        }
    }


    @Override
    public void recordDownload(String fileName, long bytesDownloaded, long startTime, long endTime, boolean failed) {
        long elapsedTimeNanos = endTime - startTime;
        RemoteStoreStat remoteStoreStat = this.remoteStoreStats.get(fileName);
        if (remoteStoreStat == null) {
            remoteStoreStat = new RemoteStoreStat();
            this.remoteStoreStats.put(fileName, remoteStoreStat);
        }
        if (elapsedTimeNanos != 0) {
            remoteStoreStat.successCount++;
            remoteStoreStat.effectiveBytes += bytesDownloaded;
            remoteStoreStat.maxElapsedTime = Math.max(remoteStoreStat.maxElapsedTime, elapsedTimeNanos);
            remoteStoreStat.minElapsedTime = Math.min(remoteStoreStat.minElapsedTime, elapsedTimeNanos);
            remoteStoreStat.sumElapasedTime += elapsedTimeNanos;
            remoteStoreStat.blocksCount++;
            this.effectiveBytes += bytesDownloaded;
            this.downloadIntervals.add(new long[]{startTime, endTime});
        } else {
            if (failed) {
                remoteStoreStat.failedCount++;
            } else {
                remoteStoreStat.cancelCount++;
            }
        }
    }

    @Override
    public void recordFullFileDownload(String fileName, long bytesDownloaded, long timeElapsedNanos, boolean failed) {
        RemoteStoreStatFullFile s3Stat = this.fullFileS3ServiceStats.get(fileName);
        if (s3Stat == null) {
            s3Stat = new RemoteStoreStatFullFile();
            this.fullFileS3ServiceStats.put(fileName, s3Stat);
        }
        if (timeElapsedNanos != 0) {
            s3Stat.successCount++;
            s3Stat.effectiveBytes += bytesDownloaded;
            s3Stat.elapsedTimeNanos += timeElapsedNanos;
            this.fullFileEffectiveBytes += bytesDownloaded;
            this.fullFileElapsedTimeNanos += timeElapsedNanos;
        } else {
            if (failed) {
                s3Stat.failedCount++;
            } else {
                s3Stat.cancelCount++;
            }
        }
    }

    @Override
    public void recordFileAccess(String blockFileName, boolean hit) {
        final FileBlock fileBlock = getFileBlock(blockFileName);
        FileCacheStat fileCacheStat = this.fileCacheStats.get(fileBlock.fileName);
        if (fileCacheStat == null) {
            fileCacheStat = new FileCacheStat();
            this.fileCacheStats.put(fileBlock.fileName, fileCacheStat);
        }
        if (hit) {
            fileCacheStat.hits++;
            this.hits++;
            fileCacheStat.hitBlocks.add(fileBlock.blockId);
        } else {
            fileCacheStat.miss++;
            this.miss++;
            fileCacheStat.missBlocks.add(fileBlock.blockId);
        }
    }

    @Override
    public void recordFullFileAccess(String fullFileName, boolean hit) {
        FileCacheStat fileCacheStat = this.fullFileCacheStats.get(fullFileName);
        if (fileCacheStat == null) {
            fileCacheStat = new FileCacheStat();
            this.fullFileCacheStats.put(fullFileName, fileCacheStat);
        }
        if (hit) {
            fileCacheStat.hits++;
            this.fullFileHits++;
        } else {
            fileCacheStat.miss++;
            this.fullFileMiss++;
        }
    }

    @Override
    public long ramBytesUsed() {
        long size = BASE_RAM_BYTES_USED;
        // While this is not completely accurate, it serves as
        // good approximation for tracking any memory leaks
        size += RamUsageEstimator.sizeOf(fileCacheStats.values().toArray(new FileCacheStat[0]));
        size += RamUsageEstimator.sizeOf(remoteStoreStats.values().toArray(new RemoteStoreStat[0]));
        return size;
    }

    // package-private for test purpose
    long getTotalDownloadTime() {
        long totalDownloadTime = 0;
        if (downloadIntervals.isEmpty()) {
            return 0;
        }

        downloadIntervals.sort((a,b) -> (int)(a[0] - b[0]));

        long currentStartTime = downloadIntervals.get(0)[0];
        long currentEndTime = downloadIntervals.get(0)[1];

        for (int i = 1; i < downloadIntervals.size(); i++) {
            long nextStartTime = downloadIntervals.get(i)[0];
            long nextEndTime = downloadIntervals.get(i)[1];

            if (nextStartTime <= currentEndTime) {
                currentEndTime = Math.max(currentEndTime, nextEndTime);
            }
            else {
                totalDownloadTime += currentEndTime - currentStartTime;
                currentStartTime = nextStartTime;
                currentEndTime = nextEndTime;
            }
        }

        totalDownloadTime += currentEndTime - currentStartTime;
        return totalDownloadTime;
    }

    @Override
    public void recordEndTime() {
        this.endTime = System.currentTimeMillis();
    }

    private String getSummary() {
        return String.format("{Remote: %d mb in %d ms, FC: %d hits out of %d total, Prefetch Files: %s, ReadAhead Files: %s, Full File Remote: %d mb in %d ms, Full File FC: %d hits out of %d total}",
            this.effectiveBytes / BYTES_IN_MB, TimeUnit.NANOSECONDS.toMillis(getTotalDownloadTime()), this.hits, this.hits + this.miss, this.prefetchFiles, this.readAheadFiles,
            this.fullFileEffectiveBytes / BYTES_IN_MB, TimeUnit.NANOSECONDS.toMillis(this.fullFileElapsedTimeNanos), this.fullFileHits, this.fullFileHits + this.fullFileMiss);
    }

    private String getDetails() {
        return String.format("{Remote: %s, FC: %s, Full File Remote: %s, Full File FC: %s}",
            this.remoteStoreStats, this.fileCacheStats, this.fullFileS3ServiceStats, this.fullFileCacheStats);
    }

    private String getTimestamps() {
        return String.format("{StartTime: %s, EndTime: %s}", this.startTime, this.endTime);
    }

    @Override
    public String toString() {
        return String.format("{ParentTask: %s, ShardId: %s, Summary: %s, Details: %s, Timestamps: %s}",
            parentTaskId, shardId, getSummary(), getDetails(), getTimestamps());
    }

    @Override
    public String getParentTaskId() {
        return parentTaskId;
    }

    @Override
    public String getShardId() {
        return shardId;
    }

    private long getSetSize(Set<Integer> set) {
        // While this is not completely accurate, it serves as
        // good approximation for tracking any memory leaks
        long size = RamUsageEstimator.shallowSizeOf(set);
        size += set.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        size += set.size() * Integer.BYTES;
        return size;
    }

    class FileBlock {
        final String fileName;
        final int blockId;
        public FileBlock(String fileName, int blockId) {
            this.fileName = fileName;
            this.blockId = blockId;
        }
    }

    /**
    //Remote Store Stat
     */
    protected class RemoteStoreStat implements Accountable {
        public long effectiveBytes;
        public long successCount;
        public long failedCount;
        public long cancelCount;
        public long maxElapsedTime;
        public long minElapsedTime;
        public long sumElapasedTime;
        public long blocksCount;

        public RemoteStoreStat() {
            this.effectiveBytes = 0L;
            this.successCount = 0L;
            this.failedCount = 0L;
            this.cancelCount = 0L;
            this.maxElapsedTime = Long.MIN_VALUE;
            this.minElapsedTime = Long.MAX_VALUE;
            this.sumElapasedTime = 0L;
            this.blocksCount = 0L;
        }

        public String toString() {
            final Map<String, Object> s3ServiceStatInfo = new HashMap<>();
            s3ServiceStatInfo.put("Total download size (in mb)", this.effectiveBytes / BYTES_IN_MB);
            final Map<String, Long> blockDownloadTimes = new HashMap<>();
            blockDownloadTimes.put("Min", TimeUnit.NANOSECONDS.toMillis(this.minElapsedTime));
            blockDownloadTimes.put("Max", TimeUnit.NANOSECONDS.toMillis(this.maxElapsedTime));
            blockDownloadTimes.put("Avg", TimeUnit.NANOSECONDS.toMillis(blocksCount == 0 ? 0 : this.sumElapasedTime / blocksCount));
            s3ServiceStatInfo.put("Block download time (in ms)", blockDownloadTimes);
            return String.format("%s", s3ServiceStatInfo);
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }
    }

    /**
     Full File Stats
    */
    protected class RemoteStoreStatFullFile implements Accountable {
        public long elapsedTimeNanos;
        public long effectiveBytes;
        public long successCount;
        public long failedCount;
        public long cancelCount;

        public RemoteStoreStatFullFile() {
            this.elapsedTimeNanos = 0L;
            this.effectiveBytes = 0L;
            this.successCount = 0L;
            this.failedCount = 0L;
            this.cancelCount = 0L;
        }

        public String toString() {
            return String.format("%d mb in %d ms", this.effectiveBytes / BYTES_IN_MB, TimeUnit.NANOSECONDS.toMillis(this.elapsedTimeNanos));
        }

        @Override
        public long ramBytesUsed() {
            return FULL_FILE_BASE_RAM_BYTES_USED;
        }
    }

    /**
    Query level hits/miss stats
     */
    protected class FileCacheStat implements Accountable {
        public long hits;
        public long miss;
        public Set<Integer> hitBlocks;
        public Set<Integer> missBlocks;

        public FileCacheStat() {
            this.hits = 0L;
            this.miss = 0L;
            this.hitBlocks = new HashSet<>();
            this.missBlocks = new HashSet<>();
        }

        public String toString() {
            // Full file case
            if (hitBlocks.isEmpty() && missBlocks.isEmpty()) {
                return String.format("%d hits out of %d total", this.hits, this.hits + this.miss);
            } else {
                return String.format("%d hits out of %d total, %d distinct hit blocks - %s, %d distinct miss blocks - %s",
                    this.hits, this.hits +
                        this.miss, this.hitBlocks.size(), this.hitBlocks, this.missBlocks.size(), this.missBlocks);
            }
        }

        @Override
        public long ramBytesUsed() {
            long size = FC_BASE_RAM_BYTES_USED;
            size += getSetSize(hitBlocks);
            size += getSetSize(missBlocks);
            return size;
        }
    }

}
