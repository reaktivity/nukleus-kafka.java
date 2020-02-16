/**
 * Copyright 2016-2020 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.kafka.internal.cache;

import java.util.LinkedHashMap;
import java.util.Map;

import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheTopicConfigSupplier;

public final class KafkaCacheBrokerWriter
{
    private final String clusterName;
    private final long brokerId;
    private final KafkaCacheSegmentSupplier segmentSupplier;
    private final KafkaCacheTopicConfigSupplier topicConfigSupplier;
    private final Map<String, KafkaCacheTopicWriter> topicsByName;

    KafkaCacheBrokerWriter(
        String clusterName,
        long brokerId,
        KafkaCacheSegmentSupplier segmentSupplier,
        KafkaCacheTopicConfigSupplier topicConfigSupplier)
    {
        this.clusterName = clusterName;
        this.brokerId = brokerId;
        this.segmentSupplier = segmentSupplier;
        this.topicConfigSupplier = topicConfigSupplier;
        this.topicsByName = new LinkedHashMap<>();
    }

    public long brokerId()
    {
        return brokerId;
    }

    public KafkaCacheTopicWriter supplyTopic(
        String topicName)
    {
        return topicsByName.computeIfAbsent(topicName, this::newTopic);
    }

    private KafkaCacheTopicWriter newTopic(
        String topicName)
    {
        return new KafkaCacheTopicWriter(clusterName, topicName, segmentSupplier, topicConfigSupplier);
    }
}
