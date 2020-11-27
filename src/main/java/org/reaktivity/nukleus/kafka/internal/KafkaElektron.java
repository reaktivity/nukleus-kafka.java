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

import static org.reaktivity.nukleus.route.RouteKind.CACHE_CLIENT;
import static org.reaktivity.nukleus.route.RouteKind.CACHE_SERVER;
import static org.reaktivity.nukleus.route.RouteKind.CLIENT;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;

import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.Elektron;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheClientFactoryBuilder;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheRoute;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheServerFactoryBuilder;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaClientFactoryBuilder;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaClientRoute;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaStreamFactoryBuilder;
import org.reaktivity.nukleus.route.AddressFactoryBuilder;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

final class KafkaElektron implements Elektron
{
    private final Long2ObjectHashMap<KafkaClientRoute> clientRoutesById;
    private final Long2ObjectHashMap<KafkaCacheRoute> cacheRoutesById;
    private final Map<RouteKind, KafkaStreamFactoryBuilder> streamFactoryBuilders;
    private final Map<RouteKind, AddressFactoryBuilder> addressFactoryBuilders;

    KafkaElektron(
        int index,
        KafkaConfiguration config,
        Function<String, KafkaCache> supplyCache)
    {
        this.clientRoutesById = new Long2ObjectHashMap<>();
        this.cacheRoutesById = new Long2ObjectHashMap<>();

        Map<RouteKind, KafkaStreamFactoryBuilder> streamFactoryBuilders = new EnumMap<>(RouteKind.class);
        streamFactoryBuilders.put(CLIENT, new KafkaClientFactoryBuilder(config, this::supplyClientRoute));
        streamFactoryBuilders.put(CACHE_SERVER, new KafkaCacheServerFactoryBuilder(config, supplyCache,
            this::supplyCacheRoute));
        streamFactoryBuilders.put(CACHE_CLIENT, new KafkaCacheClientFactoryBuilder(config, supplyCache,
            this::supplyCacheRoute, index));
        this.streamFactoryBuilders = streamFactoryBuilders;

        Map<RouteKind, AddressFactoryBuilder> addressFactoryBuilders = new EnumMap<>(RouteKind.class);
        for (Map.Entry<RouteKind, KafkaStreamFactoryBuilder> entry : streamFactoryBuilders.entrySet())
        {
            final RouteKind routeKind = entry.getKey();
            final KafkaStreamFactoryBuilder streamFactoryBuilder = entry.getValue();
            final AddressFactoryBuilder addressFactoryBuilder = streamFactoryBuilder.addressFactoryBuilder();
            if (routeKind == CACHE_SERVER && config.cacheServerBootstrap())
            {
                addressFactoryBuilders.put(routeKind, addressFactoryBuilder);
            }
        }
        this.addressFactoryBuilders = addressFactoryBuilders;
    }

    @Override
    public StreamFactoryBuilder streamFactoryBuilder(
        RouteKind kind)
    {
        return streamFactoryBuilders.get(kind);
    }

    @Override
    public AddressFactoryBuilder addressFactoryBuilder(
        RouteKind kind)
    {
        return addressFactoryBuilders.get(kind);
    }

    @Override
    public String toString()
    {
        return String.format("%s %s", getClass().getSimpleName(), streamFactoryBuilders);
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
