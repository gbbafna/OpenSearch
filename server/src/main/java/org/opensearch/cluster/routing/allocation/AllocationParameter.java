/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

/**
 * Allocation Constraint Parameters
 */
public class AllocationParameter {
    private float preferPrimaryBalanceShardBuffer;

    private float preferPrimaryBalanceIndexBuffer;

    private float shardBalanceBuffer;

    public AllocationParameter(float shardBalanceBuffer, float preferPrimaryBalanceIndexBuffer, float preferPrimaryBalanceShardBuffer) {
        this.preferPrimaryBalanceShardBuffer = preferPrimaryBalanceShardBuffer;
        this.preferPrimaryBalanceIndexBuffer = preferPrimaryBalanceIndexBuffer;
        this.shardBalanceBuffer = shardBalanceBuffer;
    }

    public float getPreferPrimaryBalanceShardBuffer() {
        return preferPrimaryBalanceShardBuffer;
    }

    public float getPreferPrimaryBalanceIndexBuffer() {
        return preferPrimaryBalanceIndexBuffer;
    }

    public float getShardBalanceBuffer() {
        return shardBalanceBuffer;
    }

}
