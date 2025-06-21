/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.test.OpenSearchTestCase;

public class WarmPerQueryMetricsImplTests  extends OpenSearchTestCase {

    public void testGetFileBlock() {
        WarmPerQueryMetricImpl w = new WarmPerQueryMetricImpl("parent", "shard-id");
        WarmPerQueryMetricImpl.FileBlock f = w.getFileBlock("/Users/gbbafna/ws/CR-191160973/src/Opensearch-StormbornPlugin/build/testrun/integTest/temp/com.amazon.stormborn.WarmIndexBasicIT_764DE3FFD3CD269B-001/tempDir-002/node_t1/nodes/0/indices/1LjwPMfJStCOYJC_undydw/0/index/_0.cfs_block_0");
        assertEquals(f.blockId, 0);
        assertEquals(f.fileName, "_0.cfs");
    }
}
