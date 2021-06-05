/**
 * Copyright 2016-2021 The Reaktivity Project
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

import static org.reaktivity.reaktor.config.Role.CACHE_CLIENT;
import static org.reaktivity.reaktor.config.Role.CACHE_SERVER;
import static org.reaktivity.reaktor.config.Role.CLIENT;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;

import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheClientFactory;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheRoute;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheServerFactory;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaClientFactory;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaClientRoute;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaStreamFactory;
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Role;
import org.reaktivity.reaktor.nukleus.Elektron;
import org.reaktivity.reaktor.nukleus.ElektronContext;
import org.reaktivity.reaktor.nukleus.stream.StreamFactory;

final class KafkaElektron implements Elektron
{
    private final Long2ObjectHashMap<KafkaClientRoute> clientRoutesById;
    private final Long2ObjectHashMap<KafkaCacheRoute> cacheRoutesById;
    private final Map<Role, KafkaStreamFactory> factories;

    KafkaElektron(
        KafkaConfiguration config,
        ElektronContext context,
        Function<String, KafkaCache> supplyCache)
    {
        this.clientRoutesById = new Long2ObjectHashMap<>();
        this.cacheRoutesById = new Long2ObjectHashMap<>();

        Map<Role, KafkaStreamFactory> factories = new EnumMap<>(Role.class);
        factories.put(CLIENT, new KafkaClientFactory(config, context, this::supplyClientRoute));
        factories.put(CACHE_SERVER, new KafkaCacheServerFactory(config, context, supplyCache,
            this::supplyCacheRoute));
        factories.put(CACHE_CLIENT, new KafkaCacheClientFactory(config, context, supplyCache,
            this::supplyCacheRoute));
        this.factories = factories;
    }

    @Override
    public StreamFactory attach(
        Binding binding)
    {
        final KafkaStreamFactory factory = factories.get(binding.kind);

        if (factory != null)
        {
            factory.attach(binding);
        }

        return factory;
    }

    @Override
    public void detach(
        Binding binding)
    {
        final KafkaStreamFactory factory = factories.get(binding.kind);

        if (factory != null)
        {
            factory.detach(binding.id);
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s %s", getClass().getSimpleName(), factories);
    }

    private KafkaCacheRoute supplyCacheRoute(
        long routeId)
    {
        return cacheRoutesById.computeIfAbsent(routeId, KafkaCacheRoute::new);
    }

    private KafkaClientRoute supplyClientRoute(
        long routeId)
    {
        return clientRoutesById.computeIfAbsent(routeId, KafkaClientRoute::new);
    }
}
