/**
 * Copyright 2016-2018 The Reaktivity Project
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

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.agrona.collections.Long2ObjectHashMap;

public class KafkaCounters
{
    private final Function<String, LongSupplier> supplyCounter;
    private final Function<String, LongConsumer> supplyAccumulator;
    private final Long2ObjectHashMap<KafkaRefCounters> countersByRoute;

    public final LongSupplier cacheHits;
    public final LongSupplier cacheMisses;
    public final LongConsumer cacheInUse;
    public final LongSupplier cacheBufferAcquires;
    public final LongSupplier cacheBufferReleases;
    public final LongSupplier dispatchNoWindow;
    public final LongSupplier dispatchNeedOtherMessage;


    public KafkaCounters(
        Function<String, LongSupplier> supplyCounter,
        Function<String, LongConsumer> supplyAccumulator)
    {
        this.supplyCounter = supplyCounter;
        this.supplyAccumulator = supplyAccumulator;
        this.countersByRoute = new Long2ObjectHashMap<>();
        this.cacheHits = supplyCounter.apply("cache.hits");
        this.cacheMisses = supplyCounter.apply("cache.misses");
        this.cacheInUse = supplyAccumulator.apply("cache.inuse");
        this.dispatchNoWindow = supplyCounter.apply("dispatch.no.window");
        this.dispatchNeedOtherMessage = supplyCounter.apply("dispatch.need.other.message");
        this.cacheBufferAcquires = supplyCounter.apply("message.cache.buffer.acquires");
        this.cacheBufferReleases = supplyCounter.apply("message.cache.buffer.releases");
    }

    public KafkaRefCounters supplyRef(
        long networkRouteId)
    {
        return countersByRoute.computeIfAbsent(networkRouteId, this::newKafkaRefCounters);
    }

    private KafkaRefCounters newKafkaRefCounters(
        long networkRouteId)
    {
        return new KafkaRefCounters(networkRouteId, supplyCounter);
    }
}
