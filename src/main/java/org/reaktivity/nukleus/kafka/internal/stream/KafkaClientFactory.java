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

import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.budget.KafkaMergedBudgetAccountant;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ExtensionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaClientFactory implements StreamFactory
{
    private final BeginFW beginRO = new BeginFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();

    private final int kafkaTypeId;
    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final Int2ObjectHashMap<StreamFactory> streamFactoriesByKind;

    public KafkaClientFactory(
        KafkaConfiguration config,
        RouteManager router,
        Signaler signaler,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        LongSupplier supplyBudgetId,
        LongFunction<BudgetDebitor> supplyDebitor,
        BudgetCreditor creditor,
        LongFunction<KafkaClientRoute> supplyClientRoute)
    {
        final Long2ObjectHashMap<MessageConsumer> correlations = new Long2ObjectHashMap<>();
        final KafkaMergedBudgetAccountant accountant = new KafkaMergedBudgetAccountant(supplyDebitor, creditor);

        final KafkaClientMetaFactory clientMetaFactory = new KafkaClientMetaFactory(
                config, router, signaler, writeBuffer, bufferPool,
                supplyInitialId, supplyReplyId, supplyTraceId,
                supplyTypeId, accountant::supplyDebitor, correlations, supplyClientRoute);

        final KafkaClientDescribeFactory clientDescribeFactory = new KafkaClientDescribeFactory(
                config, router, signaler, writeBuffer, bufferPool,
                supplyInitialId, supplyReplyId, supplyTraceId,
                supplyTypeId, accountant::supplyDebitor, correlations);

        final KafkaClientFetchFactory clientFetchFactory = new KafkaClientFetchFactory(
                config, router, signaler, writeBuffer, bufferPool,
                supplyInitialId, supplyReplyId, supplyTraceId,
                supplyTypeId, accountant::supplyDebitor, correlations, supplyClientRoute);

        final KafkaClientProduceFactory clientProduceFactory = new KafkaClientProduceFactory(
                config, router, signaler, writeBuffer, bufferPool,
                supplyInitialId, supplyReplyId, supplyTraceId,
                supplyTypeId, correlations, supplyClientRoute);

        final KafkaMergedFactory clientMergedFactory = new KafkaMergedFactory(
                config, router, signaler, writeBuffer, supplyInitialId, supplyReplyId, supplyTraceId,
                supplyTypeId, correlations, accountant.creditor());

        final Int2ObjectHashMap<StreamFactory> streamFactoriesByKind = new Int2ObjectHashMap<>();
        streamFactoriesByKind.put(KafkaBeginExFW.KIND_META, clientMetaFactory);
        streamFactoriesByKind.put(KafkaBeginExFW.KIND_DESCRIBE, clientDescribeFactory);
        streamFactoriesByKind.put(KafkaBeginExFW.KIND_FETCH, clientFetchFactory);
        streamFactoriesByKind.put(KafkaBeginExFW.KIND_PRODUCE, clientProduceFactory);
        streamFactoriesByKind.put(KafkaBeginExFW.KIND_MERGED, clientMergedFactory);

        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.correlations = correlations;
        this.streamFactoriesByKind = streamFactoriesByKind;
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer sender)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream = null;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newApplicationStream(begin, sender);
        }
        else
        {
            newStream = newNetworkStream(begin, sender);
        }

        return newStream;
    }

    private MessageConsumer newApplicationStream(
        BeginFW begin,
        MessageConsumer application)
    {
        final OctetsFW extension = begin.extension();
        final ExtensionFW beginEx = extensionRO.tryWrap(extension.buffer(), extension.offset(), extension.limit());
        final KafkaBeginExFW kafkaBeginEx = beginEx != null && beginEx.typeId() == kafkaTypeId ?
                kafkaBeginExRO.tryWrap(extension.buffer(), extension.offset(), extension.limit()) : null;

        MessageConsumer newStream = null;

        if (kafkaBeginEx != null)
        {
            final StreamFactory streamFactory = streamFactoriesByKind.get(kafkaBeginEx.kind());
            if (streamFactory != null)
            {
                newStream = streamFactory.newStream(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof(), application);
            }
        }

        return newStream;
    }

    private MessageConsumer newNetworkStream(
        BeginFW begin,
        MessageConsumer network)
    {
        final long streamId = begin.streamId();

        return correlations.remove(streamId);
    }
}
