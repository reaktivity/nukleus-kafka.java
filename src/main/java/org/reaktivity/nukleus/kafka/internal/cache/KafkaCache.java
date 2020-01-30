/**
 * Copyright 2016-2019 The Reaktivity Project
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.agrona.collections.Hashing;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;

public final class KafkaCache
{
    private final KafkaConfiguration config;
    private final Map<String, KafkaCacheCluster> clustersByName;
    private final List<MessageConsumer> clients;
    public static final String TYPE_NAME = String.format("%s/cache", KafkaNukleus.NAME);

    public KafkaCache(
        KafkaConfiguration config)
    {
        this.config = config;
        this.clustersByName = new ConcurrentHashMap<>();
        this.clients = new CopyOnWriteArrayList<>();
    }

    public KafkaCacheCluster supplyCluster(
        String clusterName)
    {
        return clustersByName.computeIfAbsent(clusterName, this::newCluster);
    }

    public void register(
        MessageConsumer client)
    {
        clients.add(client);
    }

    public void resume(
        String clusterName,
        String topicName)
    {
        clustersByName.compute(clusterName, (k, v) -> acquireTopic(k, v, topicName));
    }

    public void suspend(
        String clusterName,
        String topicName)
    {
        clustersByName.computeIfPresent(clusterName, (k, v) -> releaseTopic(v, topicName));
    }

    private KafkaCacheCluster newCluster(
        String clusterName)
    {
        return new KafkaCacheCluster(config, clusterName, this::resolveWriter, this::resolveAnyWriter);
    }

    private KafkaCacheCluster acquireTopic(
        String clusterName,
        KafkaCacheCluster cluster,
        String topicName)
    {
        if (cluster == null)
        {
            cluster = newCluster(clusterName);
        }

        cluster.acquireTopic(topicName);
        cluster.refs++;
        assert cluster.refs > 0;

        return cluster;
    }

    private KafkaCacheCluster releaseTopic(
        KafkaCacheCluster cluster,
        String topicName)
    {
        cluster.releaseTopic(topicName);
        cluster.refs--;
        return cluster.refs != 0 ? cluster : null;
    }

    private MessageConsumer resolveWriter(
        int brokerId)
    {
        final int brokerHash = Hashing.hash(brokerId);
        final int clientCount = clients.size();
        final int clientIndex = brokerHash % clientCount;
        return clients.get(clientIndex);
    }

    private MessageConsumer resolveAnyWriter()
    {
        final int clientCount = clients.size();
        final int clientIndex = ThreadLocalRandom.current().nextInt(clientCount);
        return clients.get(clientIndex);
    }
}
