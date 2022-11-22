/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.lucene.util.SetOnce;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

public class RemoteTranslogMetadata implements Writeable, Comparable<RemoteTranslogMetadata> {

    private final long primaryTerm;

    private final long generation;

    private final long minTranslogGeneration;

    private final long timeStamp;

    private final SetOnce<Map<String, Object>> generationToPrimaryTermMapper = new SetOnce<>();

    public static final String METADATA_SEPARATOR = "_";

    public static final MetadataFilenameComparator METADATA_FILENAME_COMPARATOR =
        new MetadataFilenameComparator();

    public RemoteTranslogMetadata(long primaryTerm, long generation, long minTranslogGeneration) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.minTranslogGeneration = minTranslogGeneration;
        this.timeStamp = System.currentTimeMillis();
    }

    public RemoteTranslogMetadata(StreamInput in) throws IOException {
        this.primaryTerm = in.readLong();
        this.generation = in.readLong();
        this.minTranslogGeneration = in.readLong();
        this.timeStamp = in.readLong();
        this.generationToPrimaryTermMapper.set(in.readMap());
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getGeneration() {
        return generation;
    }

    public long getMinTranslogGeneration() {
        return minTranslogGeneration;
    }

    public void setGenerationToPrimaryTermMapper(Map<String, Object> generationToPrimaryTermMap) {
        generationToPrimaryTermMapper.set(generationToPrimaryTermMap);
    }

    public Map<String, Object> getGenerationToPrimaryTermMapper() {
        return generationToPrimaryTermMapper.get();
    }

    public String getMetadataFileName() {
        return String.join(
            METADATA_SEPARATOR,
            Arrays.asList(String.valueOf(primaryTerm), String.valueOf(generation), String.valueOf(timeStamp))
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTerm, generation, timeStamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteTranslogMetadata other = (RemoteTranslogMetadata) o;
        return Objects.equals(this.primaryTerm, other.primaryTerm)
            && Objects.equals(this.generation, other.generation)
            && Objects.equals(this.timeStamp, other.timeStamp);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(primaryTerm);
        out.writeLong(generation);
        out.writeLong(minTranslogGeneration);
        out.writeLong(timeStamp);
        out.writeMap(generationToPrimaryTermMapper.get());
    }

    @Override
    public int compareTo(RemoteTranslogMetadata o) {
        return -1;
    }

    /**
     * Comparator to sort the metadata filenames. The order of sorting is: Primary Term, Generation, UUID
     * Even though UUID sort does not provide any info on recency, it provides a consistent way to sort the filenames.
     */
    static class MetadataFilenameComparator implements Comparator<String> {
        @Override
        public int compare(String first, String second) {
            String[] firstTokens = first.split(METADATA_SEPARATOR);
            String[] secondTokens = second.split(METADATA_SEPARATOR);

            long firstPrimaryTerm = getPrimaryTerm(firstTokens);
            long secondPrimaryTerm = getPrimaryTerm(secondTokens);
            if (firstPrimaryTerm != secondPrimaryTerm) {
                return getPrimaryTerm(firstTokens) > getPrimaryTerm(secondTokens) ? 1 : -1 ;
            }
            else if (!firstTokens[1].equals(secondTokens[1])) {
                long firstGeneration = getGeneration(firstTokens);
                long secondGeneration = getGeneration(secondTokens);
                if (firstGeneration != secondGeneration) {
                    return firstGeneration > secondGeneration ? 1 : -1;
                }
            }
            else if (!firstTokens[2].equals(secondTokens[2])) {
                return Long.parseLong(firstTokens[0]) > Long.parseLong(secondTokens[0]) ? 1 : -1;
            }
            return 0;
        }
    }

    // Visible for testing
    static long getPrimaryTerm(String[] filenameTokens) {
        return Long.parseLong(filenameTokens[0]);
    }

    // Visible for testing
    static long getGeneration(String[] filenameTokens) {
        return Long.parseLong(filenameTokens[1]);
    }
}
