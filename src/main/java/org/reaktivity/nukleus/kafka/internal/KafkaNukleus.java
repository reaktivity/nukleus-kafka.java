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
package org.reaktivity.nukleus.kafka.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;

public final class KafkaNukleus implements Nukleus
{
    public static final String NAME = "kafka";

    private final KafkaConfiguration config;
    private final Map<String, KafkaCache> cachesByName;

    KafkaNukleus(
        KafkaConfiguration config)
    {
        this.config = config;
        this.cachesByName = new ConcurrentHashMap<>();
    }

    @Override
    public String name()
    {
        return KafkaNukleus.NAME;
    }

    @Override
    public KafkaConfiguration config()
    {
        return config;
    }

    @Override
    public KafkaElektron supplyElektron(
        int index)
    {
        return new KafkaElektron(index, config, this::supplyCache);
    }

    public KafkaCache supplyCache(
        String name)
    {
        return cachesByName.computeIfAbsent(name, this::newCache);
    }

    private KafkaCache newCache(
        String name)
    {
        return new KafkaCache(config, name);
    }
}
