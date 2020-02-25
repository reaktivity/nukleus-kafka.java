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

import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;

public final class KafkaCache extends KafkaCacheObjects.ReadWrite<KafkaCacheView, KafkaCache>
{
    private final KafkaConfiguration config;
    private final String name;
    private final Map<String, KafkaCacheTopic> topicsByName;

    public KafkaCache(
        KafkaConfiguration config,
        String name)
    {
        this.config = config;
        this.name = name;
        this.topicsByName = new ConcurrentHashMap<>();
    }

    public KafkaCacheTopic supplyTopic(
        String name)
    {
        return topicsByName.computeIfAbsent(name, this::newTopic);
    }

    @Override
    public String toString()
    {
        return String.format("[%s] %s", getClass().getSimpleName(), name);
    }

    @Override
    protected KafkaCache self()
    {
        return this;
    }

    private KafkaCacheTopic newTopic(
            String name)
    {
        return new KafkaCacheTopic(config, name);
    }
}
