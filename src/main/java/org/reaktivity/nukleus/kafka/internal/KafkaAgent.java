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

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.memory.CountingMemoryManager;
import org.reaktivity.nukleus.kafka.internal.memory.DefaultMemoryManager;
import org.reaktivity.nukleus.kafka.internal.memory.MemoryLayout;
import org.reaktivity.nukleus.kafka.internal.memory.MemoryManager;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaError;
import org.reaktivity.nukleus.kafka.internal.stream.NetworkConnectionPool;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.util.DelayedTaskScheduler;

final class KafkaAgent implements Agent
{
    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

    private final KafkaConfiguration config;
    private final List<RouteFW> routesToProcess;

    final Long2ObjectHashMap<NetworkConnectionPool> connectionPools;
    final DelayedTaskScheduler scheduler;

    private LongFunction<NetworkConnectionPool> connectionPoolFactory;
    private MemoryManager memoryManager;
    private MemoryLayout memoryLayout;

    KafkaAgent(
        KafkaConfiguration config)
    {
        this.config = config;
        this.connectionPools = new Long2ObjectHashMap<>();
        this.scheduler = new DelayedTaskScheduler();
        this.routesToProcess = new CopyOnWriteArrayList<>();
    }

    @Override
    public int doWork() throws Exception
    {
        while (!routesToProcess.isEmpty() && connectionPoolFactory != null)
        {
            List<RouteFW> processedRoutes = new ArrayList<>(routesToProcess);
            processRoutes(processedRoutes);
            routesToProcess.removeAll(processedRoutes);
        }

        return scheduler.process();
    }

    @Override
    public void onClose()
    {
        if (memoryLayout != null)
        {
            memoryLayout.close();
        }
    }

    @Override
    public String roleName()
    {
        return String.format("%s.agent", KafkaNukleus.NAME);
    }

    boolean handleRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        boolean result = true;
        switch (msgTypeId)
        {
        case RouteFW.TYPE_ID:
            RouteFW route = routeRO.wrap(buffer, index, index + length);
            final OctetsFW extension = route.extension();
            if (extension.sizeof() > 0)
            {
                MutableDirectBuffer routeBuffer = new UnsafeBuffer(new byte[length]);
                buffer.getBytes(index,  routeBuffer, 0, length);
                routesToProcess.add(new RouteFW().wrap(routeBuffer, 0, length));
            }
            break;
        default:
            break;
        }
        return result;
    }

    private void processRoutes(
        List<RouteFW> routes)
    {
        for (RouteFW route : routes)
        {
            final OctetsFW extension = route.extension();
            if (extension.sizeof() > 0)
            {
                final KafkaRouteExFW routeEx = extension.get(routeExRO::wrap);
                final String topicName = routeEx.topicName().asString();
                final ListFW<KafkaHeaderFW> headers = routeEx.headers();

                final long networkRouteId = route.correlationId();
                final long networkRemoteId = (int)(networkRouteId >> 32) & 0xffff;

                NetworkConnectionPool connectionPool =
                        connectionPools.computeIfAbsent(networkRemoteId,
                            r -> connectionPoolFactory.apply(networkRouteId));

                if (topicName != null)
                {
                    ListFW<KafkaHeaderFW> headersCopy = new ListFW<KafkaHeaderFW>(new KafkaHeaderFW());
                    headersCopy.wrap(headers.buffer(), headers.offset(), headers.limit());
                    connectionPool.addRoute(topicName, headersCopy, config.topicBootstrapEnabled(),
                            this::onKafkaError);
                }
            }
        }
    }

    private void onKafkaError(
        KafkaError errorCode,
        String topicName)
    {
        switch (errorCode)
        {
        case UNKNOWN_TOPIC_OR_PARTITION:
            System.out.println(format(
                "WARNING: bootstrap failed for topic \"%s\" with error \"unknown topic\"",
                topicName));
            break;
        default:
            System.out.println(format(
                "WARNING: bootstrap failed while getting metadata for topic \"%s\" with error code %d",
                topicName, errorCode));
        }
    }

    void setConnectionPoolFactory(
        LongFunction<NetworkConnectionPool> connectionPoolFactory)
    {
        this.connectionPoolFactory = connectionPoolFactory;
    }

    MemoryManager supplyMemoryManager(
        KafkaCounters counters)
    {
        if (memoryManager == null)
        {
            memoryManager = createMemoryManager(counters);
        }
        return memoryManager;
    }

    private MemoryManager createMemoryManager(
        KafkaCounters counters)
    {
        MemoryManager result;
        long capacity = config.messageCacheCapacity();
        if (capacity == 0)
        {
            result = KafkaNukleusFactorySpi.OUT_OF_SPACE_MEMORY_MANAGER;
        }
        else
        {
            @SuppressWarnings("deprecation")
            final MemoryLayout memoryLayout = new MemoryLayout.Builder()
                    // TODO: non-deprecated way of getting nukleus's home directory; change name of memory0?
                    .path(config.directory().resolve("kafka").resolve("memory0"))
                    .minimumBlockSize(config.messageCacheBlockCapacity())
                    .capacity(capacity)
                    .create(true)
                    .build();
            this.memoryLayout = memoryLayout;
            MemoryManager memoryManager = new DefaultMemoryManager(memoryLayout, counters);
            result = new CountingMemoryManager(
                memoryManager,
                counters.cacheBufferAcquires,
                counters.cacheBufferReleases);
        }
        return result;
    }
}
