/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.routing.WRRWeight;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class WeightedRoundRobinMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {
    public static final String TYPE = "wrr_shard_routing";
    private  List<WRRWeight> wrrWeight;

    public List<WRRWeight> getWrrWeight() {
        return wrrWeight;
    }

    public WeightedRoundRobinMetadata setWrrWeight(List<WRRWeight> wrrWeight) {
        this.wrrWeight = wrrWeight;
        return this;
    }

    public WeightedRoundRobinMetadata(StreamInput in) throws IOException {
        this.wrrWeight = in.readList(WRRWeight::new);
    }

    public WeightedRoundRobinMetadata(List<WRRWeight> wrrWeight) {
        this.wrrWeight = Collections.unmodifiableList(wrrWeight);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        //TODO: Check if this needs to be changed
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        //TODO:
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(wrrWeight);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    public static WeightedRoundRobinMetadata fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<WRRWeight> wrrWeights = new ArrayList<>();
        return new WeightedRoundRobinMetadata(wrrWeights);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        toXContent(wrrWeight, builder, params);
        return builder;
    }

    public static void toXContent(List<WRRWeight> wrrWeight, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("weights");
        for (WRRWeight wrrWeight1 : wrrWeight) {
            builder.startObject(wrrWeight1.attributeName());
            for (Map.Entry<String, Object> entry : wrrWeight1.weights().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        builder.endObject();
    }
}
