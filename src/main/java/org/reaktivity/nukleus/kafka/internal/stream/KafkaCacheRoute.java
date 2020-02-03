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
package org.reaktivity.nukleus.kafka.internal.stream;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheClientFetchFactory.KafkaCacheClientFetchFanout;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheDescribeFactory.KafkaCacheDescribeFanout;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheMetaFactory.KafkaCacheMetaFanout;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheServerFetchFactory.KafkaCacheServerFetchFanout;

public final class KafkaCacheRoute
{
    public final long routeId;
    public final Int2ObjectHashMap<KafkaCacheDescribeFanout> describeFanoutsByTopic;
    public final Int2ObjectHashMap<KafkaCacheMetaFanout> metaFanoutsByTopic;
    public final Long2ObjectHashMap<KafkaCacheClientFetchFanout> clientFetchFanoutsByTopicPartition;
    public final Long2ObjectHashMap<KafkaCacheServerFetchFanout> serverFetchFanoutsByTopicPartition;

    public KafkaCacheRoute(
        long routeId)
    {
        this.routeId = routeId;
        this.describeFanoutsByTopic = new Int2ObjectHashMap<>();
        this.metaFanoutsByTopic = new Int2ObjectHashMap<>();
        this.clientFetchFanoutsByTopicPartition = new Long2ObjectHashMap<>();
        this.serverFetchFanoutsByTopicPartition = new Long2ObjectHashMap<>();
    }

    public int topicKey(
        String topic)
    {
        return System.identityHashCode(topic.intern());
    }

    public long topicPartitionKey(
        String topic,
        int partitionId)
    {
        return (((long) topicKey(topic)) << Integer.SIZE) | partitionId;
    }
}