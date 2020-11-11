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
package org.reaktivity.nukleus.kafka.internal.stream;

import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

public final class KafkaCacheClientFactoryBuilder implements KafkaStreamFactoryBuilder
{
    private final KafkaConfiguration config;
    private final Function<String, KafkaCache> supplyCache;
    private final LongFunction<KafkaCacheRoute> supplyCacheRoute;
    private final int index;

    private RouteManager router;
    private Signaler signaler;
    private MutableDirectBuffer writeBuffer;
    private BudgetCreditor creditor;
    private LongUnaryOperator supplyInitialId;
    private LongUnaryOperator supplyReplyId;
    private LongSupplier supplyTraceId;
    private Supplier<BufferPool> supplyBufferPool;
    private ToIntFunction<String> supplyTypeId;
    private LongSupplier supplyBudgetId;
    private LongFunction<BudgetDebitor> supplyDebitor;

    public KafkaCacheClientFactoryBuilder(
        KafkaConfiguration config,
        Function<String, KafkaCache> supplyCache,
        LongFunction<KafkaCacheRoute> supplyCacheRoute,
        int index)
    {
        this.config = config;
        this.supplyCache = supplyCache;
        this.supplyCacheRoute = supplyCacheRoute;
        this.index = index;
    }

    @Override
    public KafkaCacheClientFactoryBuilder setRouteManager(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public StreamFactoryBuilder setSignaler(
        Signaler signaler)
    {
        this.signaler = signaler;
        return this;
    }

    @Override
    public KafkaCacheClientFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        return this;
    }

    @Override
    public KafkaCacheClientFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId)
    {
        this.supplyInitialId = supplyInitialId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        this.supplyReplyId = supplyReplyId;
        return this;
    }

    @Override
    public KafkaCacheClientFactoryBuilder setTraceIdSupplier(
        LongSupplier supplyTraceId)
    {
        this.supplyTraceId = supplyTraceId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setTypeIdSupplier(
        ToIntFunction<String> supplyTypeId)
    {
        this.supplyTypeId = supplyTypeId;
        return this;
    }

    @Override
    public KafkaCacheClientFactoryBuilder setBudgetIdSupplier(
        LongSupplier supplyBudgetId)
    {
        this.supplyBudgetId = supplyBudgetId;
        return this;
    }

    @Override
    public KafkaCacheClientFactoryBuilder setBudgetCreditor(
        BudgetCreditor creditor)
    {
        this.creditor = creditor;
        return this;
    }

    @Override
    public KafkaCacheClientFactoryBuilder setBudgetDebitorSupplier(
        LongFunction<BudgetDebitor> supplyDebitor)
    {
        this.supplyDebitor = supplyDebitor;
        return this;
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        this.supplyBufferPool = supplyBufferPool;
        return this;
    }

    @Override
    public StreamFactory build()
    {
        final BufferPool bufferPool = supplyBufferPool.get();

        return new KafkaCacheClientFactory(
                config,
                router,
                signaler,
                writeBuffer,
                bufferPool,
                creditor,
                supplyInitialId,
                supplyReplyId,
                supplyTraceId,
                supplyTypeId,
                supplyBudgetId,
                supplyDebitor,
                supplyCache,
                supplyCacheRoute,
                index);
    }
}
