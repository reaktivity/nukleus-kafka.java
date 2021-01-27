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
package org.reaktivity.nukleus.kafka.internal.stream;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.route.AddressFactory;
import org.reaktivity.nukleus.route.AddressFactoryBuilder;
import org.reaktivity.nukleus.route.RouteManager;

public class KafkaCacheServerAddressFactoryBuilder implements AddressFactoryBuilder
{
    private final KafkaConfiguration config;
    private final Long2ObjectHashMap<MessageConsumer> correlations;

    private RouteManager router;
    private ToIntFunction<String> supplyTypeId;
    private LongUnaryOperator supplyInitialId;
    private LongUnaryOperator supplyReplyId;
    private LongSupplier supplyTraceId;
    private MutableDirectBuffer writeBuffer;

    public KafkaCacheServerAddressFactoryBuilder(
        KafkaConfiguration config,
        Long2ObjectHashMap<MessageConsumer> correlations)
    {
        this.config = config;
        this.correlations = correlations;
    }

    @Override
    public AddressFactoryBuilder setRouter(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public AddressFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        return this;
    }

    @Override
    public AddressFactoryBuilder setTypeIdSupplier(
        ToIntFunction<String> supplyTypeId)
    {
        this.supplyTypeId = supplyTypeId;
        return this;
    }

    @Override
    public AddressFactoryBuilder setTraceIdSupplier(
        LongSupplier supplyTraceId)
    {
        this.supplyTraceId = supplyTraceId;
        return this;
    }

    @Override
    public AddressFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId)
    {
        this.supplyInitialId = supplyInitialId;
        return this;
    }

    @Override
    public AddressFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        this.supplyReplyId = supplyReplyId;
        return this;
    }

    @Override
    public AddressFactory build()
    {
        return new KafkaCacheServerAddressFactory(
            config,
            router,
            writeBuffer,
            supplyTypeId,
            supplyTraceId,
            supplyInitialId,
            supplyReplyId,
            correlations);
    }
}
