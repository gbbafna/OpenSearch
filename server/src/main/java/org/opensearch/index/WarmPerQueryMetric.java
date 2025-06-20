package org.opensearch.index;

import org.apache.lucene.util.Accountable;

/**
 * Interface that needs to be implemented by any per query metric collector
 */
public interface WarmPerQueryMetric extends Accountable {

    void recordDownload(String fileName, long effectiveBytes, long startTime, long endTime, boolean failed);

    void recordFullFileDownload(String fileName, long effectiveBytes, long elapsedTime, boolean failed);

    void recordFileAccess(String blockFileName, boolean hit);

    void recordFullFileAccess(String fullFileName, boolean hit);

    void recordEndTime();

    String getParentTaskId();

    String getShardId();
}
