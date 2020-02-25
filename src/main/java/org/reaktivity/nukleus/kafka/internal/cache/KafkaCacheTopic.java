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

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;

public final class KafkaCacheTopic extends KafkaCacheObjects.ReadWrite<KafkaCacheTopicView, KafkaCacheTopic>
{
    private final Path location;
    private final String name;
    private final KafkaCacheTopicConfig config;
    private final Map<Integer, KafkaCachePartition> partitionsById;

    public KafkaCacheTopic(
        KafkaConfiguration config,
        String name)
    {
        this.location = config.cacheDirectory();
        this.config = new KafkaCacheTopicConfig(config);
        this.name = name;
        this.partitionsById = new ConcurrentHashMap<>();
    }

    public String name()
    {
        return name;
    }

    public KafkaCacheTopicConfig config()
    {
        return config;
    }

    public KafkaCachePartition supplyPartition(
        int id)
    {
        return partitionsById.computeIfAbsent(id, this::newPartition);
    }

    @Override
    public String toString()
    {
        return String.format("[%s] %s +%d", getClass().getSimpleName(), name, references());
    }

    @Override
    protected KafkaCacheTopic self()
    {
        return this;
    }

    @Override
    protected void onClosed()
    {
        partitionsById.values().forEach(KafkaCachePartition::close);
    }

    private KafkaCachePartition newPartition(
        int id)
    {
        return new KafkaCachePartition(location, config, name, id);
    }
}
