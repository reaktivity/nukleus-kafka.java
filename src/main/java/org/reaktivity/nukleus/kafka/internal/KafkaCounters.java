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

import org.agrona.collections.Long2ObjectHashMap;

import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;

public class KafkaCounters
{
    private final Function<String, LongSupplier> supplyCounter;
    private final Map<String, Long2ObjectHashMap<KafkaRefCounters>> countersByRef;

    public KafkaCounters(
        Function<String, LongSupplier> supplyCounter,
        Map<String, Long2ObjectHashMap<KafkaRefCounters>> countersByRef)
    {
        this.supplyCounter = supplyCounter;
        this.countersByRef = countersByRef;
    }

    public KafkaRefCounters supplyRef(
        String networkName,
        long networkRef)
    {
        Long2ObjectHashMap<KafkaRefCounters> refCounters =
                countersByRef.computeIfAbsent(networkName, name -> new Long2ObjectHashMap<>());
        return refCounters.computeIfAbsent(networkRef, ref -> new KafkaRefCounters(networkName, networkRef, supplyCounter));
    }

    private KafkaRefCounters newRouteCounters(
        String networkName,
        long networkRef)
    {
        return new KafkaRefCounters(networkName, networkRef, supplyCounter);
    }
}
