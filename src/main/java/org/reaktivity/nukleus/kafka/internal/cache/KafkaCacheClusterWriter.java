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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class KafkaCacheClusterWriter
{
    private final String clusterName;
    private final KafkaCacheSegmentSupplier segmentSupplier;
    private final Map<Long, KafkaCacheBrokerWriter> brokersById;
    private final Map<String, KafkaCacheTopicWriter> topicsByName;

    KafkaCacheClusterWriter(
        String clusterName,
        KafkaCacheSegmentSupplier segmentSupplier)
    {
        this.clusterName = clusterName;
        this.segmentSupplier = segmentSupplier;
        this.brokersById = new ConcurrentHashMap<>();
        this.topicsByName = new ConcurrentHashMap<>();
    }

    public KafkaCacheBrokerWriter supplyBroker(
        long brokerId)
    {
        return brokersById.computeIfAbsent(brokerId, this::newBroker);
    }

    public KafkaCacheTopicWriter supplyTopic(
        String topicName)
    {
        return topicsByName.computeIfAbsent(topicName, this::newTopic);
    }

    private KafkaCacheBrokerWriter newBroker(
        long brokerId)
    {
        return new KafkaCacheBrokerWriter(clusterName, brokerId, segmentSupplier);
    }

    private KafkaCacheTopicWriter newTopic(
        String topicName)
    {
        return new KafkaCacheTopicWriter(clusterName, topicName, segmentSupplier);
    }
}
