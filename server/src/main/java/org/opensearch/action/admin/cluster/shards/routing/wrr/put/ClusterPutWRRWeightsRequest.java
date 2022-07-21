/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.cluster.routing.WRRWeight;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Request to update weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterPutWRRWeightsRequest extends AcknowledgedRequest<ClusterPutWRRWeightsRequest> {

    // TODO: remove this field
    private String attributeName;

    private List<WRRWeight> wrrWeight;

    public List<WRRWeight> wrrWeight() {
        return wrrWeight;
    }

    public ClusterPutWRRWeightsRequest wrrWeight(List<WRRWeight> wrrWeight) {
        this.wrrWeight = wrrWeight;
        return this;
    }

    public ClusterPutWRRWeightsRequest(StreamInput in) throws IOException {
        super(in);
        attributeName = in.readString();
        wrrWeight = in.readList(WRRWeight::new);
    }

    public ClusterPutWRRWeightsRequest() {
        wrrWeight = new ArrayList<>();
    }

    public String attributeName() {
        return attributeName;
    }

    public ClusterPutWRRWeightsRequest attributeName(String attributeName) {
        this.attributeName = attributeName;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     *
     * @param wrrWeightsDefinition weights definition from request body
     * @return this request
     */
    public ClusterPutWRRWeightsRequest source(Map<String, Object> wrrWeightsDefinition) {
        Map<String, Object> weights;
        String attributeName;
        for (Map.Entry<String, Object> entry : wrrWeightsDefinition.entrySet()) {
            attributeName = entry.getKey();
            if (!(entry.getValue() instanceof Map)) {
                throw new IllegalArgumentException("Malformed weights definition, should include an inner object");
            }
            weights = (Map<String, Object>) entry.getValue();
            wrrWeight.add(new WRRWeight(attributeName, weights));
        }
        return this;
    }

}
