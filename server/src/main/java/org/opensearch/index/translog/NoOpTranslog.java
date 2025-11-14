/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.lease.Releasable;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class NoOpTranslog extends Translog {
    public NoOpTranslog(TranslogConfig config, String translogUUID, TranslogDeletionPolicy deletionPolicy, LongSupplier globalCheckpointSupplier, LongSupplier primaryTermSupplier, LongConsumer persistedSequenceNumberConsumer, TranslogOperationHelper translogOperationHelper, ChannelFactory channelFactory) throws IOException {
        super(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer, translogOperationHelper, channelFactory);
    }


    @Override
    public boolean ensureSynced(Location location) throws IOException {
        return false;
    }

    @Override
    Releasable drainSync() {
        return null;
    }

    public TranslogStats stats() {
        // acquire lock to make the two numbers roughly consistent (no file change half way)
            return new TranslogStats();
        }

    public void close() throws IOException {
    }
}
