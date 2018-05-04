/**
 * Copyright 2016-2017 The Reaktivity Project
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

import static java.lang.String.format;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.UNKNOWN_TOPIC_OR_PARTITION;
import static org.reaktivity.nukleus.route.RouteKind.CLIENT;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.memory.DefaultMemoryManager;
import org.reaktivity.nukleus.kafka.internal.memory.MemoryLayout;
import org.reaktivity.nukleus.kafka.internal.stream.ClientStreamFactoryBuilder;
import org.reaktivity.nukleus.kafka.internal.stream.NetworkConnectionPool;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;

public final class KafkaNukleusFactorySpi implements NukleusFactorySpi, Nukleus
{
    private static final MessagePredicate DEFAULT_ROUTE_HANDLER = (m, b, i, l) ->
    {
        return true;
    };
    private static final Consumer<BiFunction<String, Long, NetworkConnectionPool>>
        DEFAULT_CONNECT_POOL_FACTORY_CONSUMER = f -> {};

    private final Map<String, Long2ObjectHashMap<NetworkConnectionPool>> connectionPools = new LinkedHashMap<>();

    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

    private List<RouteFW> routesToBootstrap = new ArrayList<>();
    private BiFunction<String, Long, NetworkConnectionPool> createNetworkConnectionPool;

    @Override
    public String name()
    {
        return "kafka";
    }

    @Override
    public Nukleus create(
        Configuration config,
        NukleusBuilder builder)
    {
        KafkaConfiguration kafkaConfig = new KafkaConfiguration(config);

        Consumer<BiFunction<String, Long, NetworkConnectionPool>> connectionPoolFactoryConsumer =
            DEFAULT_CONNECT_POOL_FACTORY_CONSUMER;
        MessagePredicate routeHandler = DEFAULT_ROUTE_HANDLER;

        final MemoryLayout memoryLayout = new MemoryLayout.Builder()
                // TODO: non-deprecated way of getting nukleus's home directory; change name of memory0?
                .path(config.directory().resolve("kafka").resolve("memory0"))
                .minimumBlockSize(kafkaConfig.messageCacheCapacity())
                .maximumBlockSize(kafkaConfig.messageCacheBlockCapacity())
                .create(true)
                .build();
        final DefaultMemoryManager memoryManager = new DefaultMemoryManager(memoryLayout);

        if (kafkaConfig.topicBootstrapEnabled())
        {
            routeHandler = this::handleRouteForBootstrap;
            connectionPoolFactoryConsumer = f -> createNetworkConnectionPool = f;
        }

        ClientStreamFactoryBuilder streamFactoryBuilder = new ClientStreamFactoryBuilder(kafkaConfig,
                memoryManager, connectionPools, connectionPoolFactoryConsumer);

        return builder.streamFactory(CLIENT, streamFactoryBuilder)
                      .routeHandler(CLIENT, routeHandler)
                      .inject(this)
                      .build();
    }

    public boolean handleRouteForBootstrap(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        boolean result = true;
        switch(msgTypeId)
        {
        case RouteFW.TYPE_ID:
            {
                RouteFW route = routeRO.wrap(buffer, index, index + length);
                final OctetsFW extension = route.extension();
                if (extension.sizeof() > 0)
                {
                    MutableDirectBuffer routeBuffer = new UnsafeBuffer(new byte[length]);
                    buffer.getBytes(index,  routeBuffer, 0, length);
                    routesToBootstrap.add(new RouteFW().wrap(routeBuffer, 0, length));
                }
            }
            break;
        default:
            break;
        }
        return result;
    }

    @Override
    public int process()
    {
        if (!routesToBootstrap.isEmpty() && createNetworkConnectionPool != null)
        {
            startTopicBootstrap(routesToBootstrap);
            routesToBootstrap.clear();
        }
        return 0;
    }

    public void startTopicBootstrap(
        List<RouteFW> routes)
    {
        for (RouteFW route : routes)
        {
            final OctetsFW extension = route.extension();
            if (extension.sizeof() > 0)
            {
                final KafkaRouteExFW routeEx = extension.get(routeExRO::wrap);
                final String topicName = routeEx.topicName().asString();

                final String networkName = route.target().asString();
                final long networkRef = route.targetRef();

                Long2ObjectHashMap<NetworkConnectionPool> connectionPoolsByRef =
                    connectionPools.computeIfAbsent(networkName, name -> new Long2ObjectHashMap<>());

                NetworkConnectionPool connectionPool = connectionPoolsByRef.computeIfAbsent(networkRef, ref ->
                    createNetworkConnectionPool.apply(networkName, networkRef));

                connectionPool.doBootstrap(topicName, errorCode ->
                {
                    switch(errorCode)
                    {
                    case UNKNOWN_TOPIC_OR_PARTITION:
                        System.out.println(format(
                            "WARNING: bootstrap failed for topic \"%s\" with error \"unknown topic\"",
                            topicName));
                        break;
                    default:
                        throw new IllegalStateException(format(
                            "Received error code %d from Kafka while attempting to bootstrap topic \"%s\"",
                            errorCode, topicName));
                    }
                });
            }
        }
    }
}
