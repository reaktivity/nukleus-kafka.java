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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;

public final class KafkaCacheCluster
{
    private final KafkaConfiguration config;
    private final String clusterName;
    private final IntFunction<MessageConsumer> supplyWriter;
    private final Supplier<MessageConsumer> supplyAnyWriter;
    private final Map<Integer, KafkaCacheBroker> brokersById;
    private final Map<String, KafkaCacheBootstrapMetaClient> clientsByTopicName;

    int refs;

    public KafkaCacheCluster(
        KafkaConfiguration config,
        String clusterName,
        IntFunction<MessageConsumer> supplyWriter,
        Supplier<MessageConsumer> supplyAnyWriter)
    {
        this.config = config;
        this.clusterName = clusterName;
        this.supplyWriter = supplyWriter;
        this.supplyAnyWriter = supplyAnyWriter;
        this.brokersById = new ConcurrentHashMap<>();
        this.clientsByTopicName = new ConcurrentHashMap<>();
    }

    public KafkaCacheBroker supplyBroker(
        int brokerId)
    {
        return brokersById.computeIfAbsent(brokerId, this::newBroker);
    }

    public KafkaCacheBroker newBroker(
        int brokerId)
    {
        return new KafkaCacheBroker(config, clusterName);
    }

    public void acquireTopic(
        String topicName)
    {
        clientsByTopicName.compute(topicName, this::acquireClient);
    }

    public void releaseTopic(
        String topicName)
    {
        clientsByTopicName.compute(topicName, this::releaseClient);
    }

    private KafkaCacheBootstrapMetaClient acquireClient(
        String topicName,
        KafkaCacheBootstrapMetaClient client)
    {
        if (client == null)
        {
            client = new KafkaCacheBootstrapMetaClient();
        }

        if (client.refs == 0)
        {
            client.doInitialBegin();
        }

        client.refs++;
        assert client.refs > 0;

        return client;
    }

    private KafkaCacheBootstrapMetaClient releaseClient(
        String topicName,
        KafkaCacheBootstrapMetaClient client)
    {
        if (client != null)
        {
            client.refs--;

            if (client.refs == 0)
            {
                client.doInitialEnd();
                client = null;
            }
        }

        return client;
    }

    private final class KafkaCacheBootstrapMetaClient
    {
        private final MessageConsumer receiver;

        private int refs;

        KafkaCacheBootstrapMetaClient()
        {
            this.receiver = supplyAnyWriter.get();
        }

        void doInitialBegin()
        {
            // send BEGIN w/ KafkaBeginEx (simulate Kafka Cache Meta Server)
            // register correlation
        }

        void doInitialEnd()
        {
            // send END
        }

        void onReplyMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            // onData -> KafkaMetaDataEx -> initiate KafkaCacheBootstrapFetchClient x M
            // react to changes to topic partition affinity, or new partitions accordingly
            // KafkaCacheBootstrapFetchClient will receive SIGNAL not DATA for messages
        }
    }
}
