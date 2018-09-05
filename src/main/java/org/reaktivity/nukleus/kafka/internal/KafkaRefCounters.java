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

import java.util.function.Function;
import java.util.function.LongSupplier;

import static java.lang.String.format;

public class KafkaRefCounters
{
    public final LongSupplier historicalFetches;
    public final LongSupplier metadataRequestIdleTimeouts;
    public final LongSupplier describeConfigsRequestIdleTimeouts;
    public final LongSupplier listOffsetsRequestIdleTimeouts;
    public final LongSupplier fetchRequestIdleTimeouts;

    KafkaRefCounters(
        String networkName,
        long networkRef,
        Function<String, LongSupplier> supplyCounter)
    {
        this.historicalFetches = supplyCounter.apply(format("historical.fetches.%s.%d", networkName, networkRef));
        this.metadataRequestIdleTimeouts = supplyCounter.apply(
                format("metadata.request.idle.timeouts.%s.%d", networkName, networkRef));
        this.describeConfigsRequestIdleTimeouts = supplyCounter.apply(
                format("describe.configs.request.idle.timeouts.%s.%d", networkName, networkRef));
        this.listOffsetsRequestIdleTimeouts = supplyCounter.apply(
                format("list.offsets.request.idle.timeouts.%s.%d", networkName, networkRef));
        this.fetchRequestIdleTimeouts = supplyCounter.apply(
                format("fetch.request.idle.timeouts.%s.%d", networkName, networkRef));
    }
}
