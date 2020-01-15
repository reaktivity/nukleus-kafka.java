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
package org.reaktivity.nukleus.kafka.internal;

import java.util.Objects;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.route.RouteKind;

public final class KafkaNukleus implements Nukleus
{
    public static final String NAME = "kafka";

    private final RouteFW routeRO = new RouteFW();
    private final UnrouteFW unrouteRO = new UnrouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

    private final KafkaConfiguration config;
    private final KafkaCache cache;
    private final Long2ObjectHashMap<KafkaBootstrap> bootstrapByRouteId;

    KafkaNukleus(
        KafkaConfiguration config)
    {
        this.config = config;
        this.cache = new KafkaCache(config);
        this.bootstrapByRouteId = new Long2ObjectHashMap<>();
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
        return new KafkaElektron(config, cache, index);
    }

    public MessagePredicate routeHandler(
        RouteKind kind)
    {
        MessagePredicate routeHandler;

        switch (kind)
        {
        case CACHE_SERVER:
            routeHandler = this::handleCacheServer;
            break;
        case CACHE_CLIENT:
            routeHandler = this::handleCacheClient;
            break;
        default:
            routeHandler = null;
            break;
        }

        return routeHandler;
    }

    private boolean handleCacheServer(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case RouteFW.TYPE_ID:
            RouteFW route = routeRO.wrap(buffer, index, index + length);
            onRouteCacheServer(route);
            break;
        case UnrouteFW.TYPE_ID:
            final UnrouteFW unroute = unrouteRO.wrap(buffer, index, index + length);
            onUnrouteCacheServer(unroute);
            break;
        }

        return true;
    }

    private void onRouteCacheServer(
        RouteFW route)
    {
        // TODO
    }

    private void onUnrouteCacheServer(
        UnrouteFW route)
    {
        // TODO
    }

    private boolean handleCacheClient(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case RouteFW.TYPE_ID:
            final RouteFW route = routeRO.wrap(buffer, index, index + length);
            onRouteCacheClient(route);
            break;
        case UnrouteFW.TYPE_ID:
            final UnrouteFW unroute = unrouteRO.wrap(buffer, index, index + length);
            onUnrouteCacheClient(unroute);
            break;
        }

        return true;
    }

    private void onRouteCacheClient(
        RouteFW route)
    {
        final String clusterName = route.localAddress().asString();
        String topicName = null;

        final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
        if (routeEx != null)
        {
            topicName = routeEx.topic().asString();
        }

        if (topicName != null)
        {
            final KafkaBootstrap bootstrap = new KafkaBootstrap(clusterName, topicName);
            final long routeId = route.correlationId();
            bootstrapByRouteId.put(routeId, bootstrap);

            bootstrap.resume();
        }
    }

    private void onUnrouteCacheClient(
        UnrouteFW unroute)
    {
        final long routeId = unroute.correlationId();

        final KafkaBootstrap bootstrap = bootstrapByRouteId.remove(routeId);
        if (bootstrap != null)
        {
            bootstrap.suspend();
        }
    }

    private final class KafkaBootstrap
    {
        final String clusterName;
        final String topicName;

        KafkaBootstrap(
            String clusterName,
            String topicName)
        {
            this.clusterName = Objects.requireNonNull(clusterName);
            this.topicName = Objects.requireNonNull(topicName);
        }

        void resume()
        {
            cache.resume(clusterName, topicName);
        }

        void suspend()
        {
            cache.suspend(clusterName, topicName);
        }
    }
}
