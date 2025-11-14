/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class NoOpTranslogFactory implements TranslogFactory {
    @Override
    public Translog newTranslog(TranslogConfig config, String translogUUID, TranslogDeletionPolicy deletionPolicy, LongSupplier globalCheckpointSupplier, LongSupplier primaryTermSupplier, LongConsumer persistedSequenceNumberConsumer, BooleanSupplier startedPrimarySupplier) throws IOException {
        return new NoOpTranslog(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer,  TranslogOperationHelper.DEFAULT, null);
    }

    @Override
    public Translog newTranslog(TranslogConfig config, String translogUUID, TranslogDeletionPolicy deletionPolicy, LongSupplier globalCheckpointSupplier, LongSupplier primaryTermSupplier, LongConsumer persistedSequenceNumberConsumer, BooleanSupplier startedPrimarySupplier, TranslogOperationHelper translogOperationHelper) throws IOException {
        return new NoOpTranslog(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer,  TranslogOperationHelper.DEFAULT, null);
    }
}
