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

public final class KafkaCacheTopicView extends KafkaCacheObjects.ReadOnly
{
    private final KafkaCacheTopic topic;
    private final Int2ObjectHashMap<KafkaCachePartitionView> partitionsById;

    public KafkaCacheTopicView(
        KafkaCacheTopic topic)
    {
        super(topic);
        this.topic = topic;
        this.partitionsById = new Int2ObjectHashMap<>();
    }

    public KafkaCachePartitionView supplyPartitionView(
        int index)
    {
        return partitionsById.computeIfAbsent(index, this::newPartitionView);
    }

    public String name()
    {
        return topic.name();
    }

    @Override
    public String toString()
    {
        return String.format("[%s] %s", getClass().getSimpleName(), name());
    }

    @Override
    protected void onClosed()
    {
        partitionsById.values().forEach(KafkaCachePartitionView::close);
    }

    private KafkaCachePartitionView newPartitionView(
        int id)
    {
        KafkaCachePartition partition = topic.supplyPartition(id);
        return partition.acquire(KafkaCachePartitionView::new);
    }
}
