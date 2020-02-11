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

import org.agrona.collections.Int2ObjectHashMap;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSentinelSegment;

public final class KafkaCacheTopicWriter
{
    private final String clusterName;
    private final String topicName;
    private final KafkaCacheSegmentSupplier segmentSupplier;
    private final Int2ObjectHashMap<KafkaCachePartitionWriter> partitionsById;

    KafkaCacheTopicWriter(
        String clusterName,
        String topicName,
        KafkaCacheSegmentSupplier segmentSupplier)
    {
        this.clusterName = clusterName;
        this.topicName = topicName;
        this.segmentSupplier = segmentSupplier;
        this.partitionsById = new Int2ObjectHashMap<>();
    }

    public String name()
    {
        return topicName;
    }

    public KafkaCachePartitionWriter supplyPartition(
        int partitionId)
    {
        return partitionsById.computeIfAbsent(partitionId, this::newPartition);
    }

    private KafkaCachePartitionWriter newPartition(
        int partitionId)
    {
        final KafkaCacheSentinelSegment sentinel = segmentSupplier.supply(clusterName, topicName, partitionId);
        return new KafkaCachePartitionWriter(partitionId, sentinel);
    }
}
