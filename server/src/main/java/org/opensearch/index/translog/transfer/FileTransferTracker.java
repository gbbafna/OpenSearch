/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.shard.ShardId;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.listener.FileTransferListener;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class FileTransferTracker implements FileTransferListener {

    private final Map<String, TransferState> fileTransferTracker;
    private final ShardId shardId;

    public FileTransferTracker(ShardId shardId) {
        this.shardId = shardId;
        this.fileTransferTracker = new ConcurrentHashMap<>();
    }

    @Override
    public void onSuccess(TransferFileSnapshot fileSnapshot) {
        TransferState targetState = TransferState.SUCCESS;
        fileTransferTracker.compute(fileSnapshot.getName(), (k, v) -> {
            if (v == null || v.validateNextState(targetState)) {
                return targetState;
            }
            throw new IllegalStateException("Unexpected transfer state " + v + "while setting target to" + targetState);
        });
    }

    @Override
    public void onFailure(TransferFileSnapshot fileSnapshot, Exception e) {
        TransferState targetState = TransferState.FAILED;
        fileTransferTracker.compute(fileSnapshot.getName(), (k, v) -> {
            if (v == null || v.validateNextState(targetState)) {
                return targetState;
            }
            throw new IllegalStateException("Unexpected transfer state " + v + "while setting target to" + targetState);
        });
    }

    public Set<TransferFileSnapshot> exclusionFilter(Set<TransferFileSnapshot> original) {
        return original.stream()
            .filter(fileSnapshot -> fileTransferTracker.get(fileSnapshot.getName()) != TransferState.SUCCESS)
            .collect(Collectors.toSet());
    }

    public Set<String> allUploaded() {
        Set<String> successFileTransferTracker = new HashSet<>();
        fileTransferTracker.forEach( (k, v) -> {
            if (v == TransferState.SUCCESS) {
                successFileTransferTracker.add(k);
            }
        });
        return successFileTransferTracker;
    }

    public enum TransferState {
        INIT,
        STARTED,
        SUCCESS,
        FAILED,
        DELETED;

        public boolean validateNextState(TransferState target) {
            switch (this) {
                case INIT:
                    return Set.of(STARTED, SUCCESS, FAILED, DELETED).contains(target);
                case STARTED:
                    return Set.of(SUCCESS, FAILED, DELETED).contains(target);
                case SUCCESS:
                case FAILED:
                    return Set.of(DELETED).contains(target);
            }
            return false;
        }
    }
}
