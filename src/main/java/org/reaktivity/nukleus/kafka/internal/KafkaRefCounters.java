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

import java.util.function.Function;
import java.util.function.LongSupplier;

import static java.lang.String.format;

public class KafkaRefCounters
{
    public final LongSupplier liveFetches;
    public final LongSupplier historicalFetches;
    public final LongSupplier metadataRequestIdleTimeouts;
    public final LongSupplier describeConfigsRequestIdleTimeouts;
    public final LongSupplier listOffsetsRequestIdleTimeouts;
    public final LongSupplier fetchRequestIdleTimeouts;
    public final LongSupplier forcedDetaches;
    public final LongSupplier internalErrors;

    KafkaRefCounters(
        long networkRouteId,
        Function<String, LongSupplier> supplyCounter)
    {
        this.liveFetches = supplyCounter.apply(format("kafka.live.fetches.%d", networkRouteId));
        this.historicalFetches = supplyCounter.apply(format("kafka.historical.fetches.%d", networkRouteId));
        this.metadataRequestIdleTimeouts = supplyCounter.apply(
                format("kafka.metadata.request.idle.timeouts.%d", networkRouteId));
        this.describeConfigsRequestIdleTimeouts = supplyCounter.apply(
                format("kafka.describe.configs.request.idle.timeouts.%d", networkRouteId));
        this.listOffsetsRequestIdleTimeouts = supplyCounter.apply(
                format("kafka.list.offsets.request.idle.timeouts.%d", networkRouteId));
        this.fetchRequestIdleTimeouts = supplyCounter.apply(
                format("kafka.fetch.request.idle.timeouts.%d", networkRouteId));
        this.forcedDetaches = supplyCounter.apply(
                format("kafka.forced.detaches.%d", networkRouteId));
        this.internalErrors = supplyCounter.apply(
                format("kafka.internal.errors.%d", networkRouteId));
    }
}
