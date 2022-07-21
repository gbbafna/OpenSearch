/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.get;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.cluster.routing.WRRWeight;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to get weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterGetWRRWeightsRequest extends ClusterManagerNodeReadRequest<ClusterGetWRRWeightsRequest> {

    private List<WRRWeight> weights = new ArrayList<>();

    public ClusterGetWRRWeightsRequest() {
    }

    public ClusterGetWRRWeightsRequest(List<WRRWeight> weights) {
        this.weights = weights;
    }

    public ClusterGetWRRWeightsRequest(StreamInput in) throws IOException {
        super(in);
        weights = in.readList(WRRWeight::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(weights);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (weights == null) {
            validationException = addValidationError("weights is null", validationException);
        }
        return validationException;
    }

    public List<WRRWeight> weights() {
        return this.weights;
    }

    public ClusterGetWRRWeightsRequest weights(List<WRRWeight> weights) {
        this.weights = weights;
        return this;
    }

}
