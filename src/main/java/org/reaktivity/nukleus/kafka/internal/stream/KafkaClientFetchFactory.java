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

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.kafka.internal.types.ProxyAddressProtocol.STREAM;
import static org.reaktivity.nukleus.kafka.internal.types.codec.offsets.IsolationLevel.READ_UNCOMMITTED;
import static org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW.Builder.DEFAULT_DELTA_TYPE;

import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongLongConsumer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;
import org.reaktivity.nukleus.kafka.internal.types.Varint32FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.RequestHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.ResponseHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TransactionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.message.MessageHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.message.RecordBatchFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.message.RecordHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.message.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.message.RecordTrailerFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offsets.OffsetsPartitionRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offsets.OffsetsPartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offsets.OffsetsRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offsets.OffsetsResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offsets.OffsetsTopicRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offsets.OffsetsTopicResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ExtensionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFetchBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaResetExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ProxyBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaClientFetchFactory implements StreamFactory
{
    private static final int FIELD_LIMIT_RECORD_BATCH_LENGTH = RecordBatchFW.FIELD_OFFSET_LEADER_EPOCH;

    private static final int ERROR_NONE = 0;
    private static final int ERROR_OFFSET_OUT_OF_RANGE = 1;
    private static final int ERROR_NOT_LEADER_FOR_PARTITION = 6;

    private static final int FLAG_CONT = 0x00;
    private static final int FLAG_FIN = 0x01;
    private static final int FLAG_INIT = 0x02;

    private static final long OFFSET_LIVE = KafkaOffsetType.LIVE.value();
    private static final long OFFSET_HISTORICAL = KafkaOffsetType.HISTORICAL.value();

    private static final int SIGNAL_NEXT_REQUEST = 1;

    private static final DirectBuffer EMPTY_BUFFER = new UnsafeBuffer();
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(EMPTY_BUFFER, 0, 0);
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};

    private static final short OFFSETS_API_KEY = 2;
    private static final short OFFSETS_API_VERSION = 2;

    private static final short FETCH_API_KEY = 1;
    private static final short FETCH_API_VERSION = 5;

    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW kafkaRouteExRO = new KafkaRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();
    private final SignalFW signalRO = new SignalFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final FlushFW.Builder flushRW = new FlushFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();
    private final KafkaDataExFW.Builder kafkaDataExRW = new KafkaDataExFW.Builder();
    private final KafkaResetExFW.Builder kafkaResetExRW = new KafkaResetExFW.Builder();
    private final ProxyBeginExFW.Builder proxyBeginExRW = new ProxyBeginExFW.Builder();

    private final RequestHeaderFW.Builder requestHeaderRW = new RequestHeaderFW.Builder();
    private final OffsetsRequestFW.Builder offsetsRequestRW = new OffsetsRequestFW.Builder();
    private final OffsetsTopicRequestFW.Builder offsetsTopicRequestRW = new OffsetsTopicRequestFW.Builder();
    private final OffsetsPartitionRequestFW.Builder offsetsPartitionRequestRW = new OffsetsPartitionRequestFW.Builder();
    private final FetchRequestFW.Builder fetchRequestRW = new FetchRequestFW.Builder();
    private final TopicRequestFW.Builder fetchTopicRequestRW = new TopicRequestFW.Builder();
    private final PartitionRequestFW.Builder fetchPartitionRequestRW = new PartitionRequestFW.Builder();

    private final ResponseHeaderFW responseHeaderRO = new ResponseHeaderFW();
    private final OffsetsResponseFW offsetsResponseRO = new OffsetsResponseFW();
    private final OffsetsTopicResponseFW offsetsTopicResponseRO = new OffsetsTopicResponseFW();
    private final OffsetsPartitionResponseFW offsetsPartitionResponseRO = new OffsetsPartitionResponseFW();
    private final FetchResponseFW fetchResponseRO = new FetchResponseFW();
    private final TopicResponseFW topicResponseRO = new TopicResponseFW();
    private final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    private final TransactionResponseFW transactionResponseRO = new TransactionResponseFW();
    private final RecordSetFW recordSetRO = new RecordSetFW();
    private final RecordBatchFW recordBatchRO = new RecordBatchFW();
    private final Varint32FW recordLengthRO = new Varint32FW();
    private final RecordHeaderFW recordHeaderRO = new RecordHeaderFW();
    private final RecordTrailerFW recordTrailerRO = new RecordTrailerFW();
    private final MessageHeaderFW messageHeaderRO = new MessageHeaderFW();
    private final OctetsFW valueRO = new OctetsFW();
    private final DirectBuffer headersRO = new UnsafeBuffer();

    private final KafkaFetchClientDecoder decodeOffsetsResponse = this::decodeOffsetsResponse;
    private final KafkaFetchClientDecoder decodeOffsets = this::decodeOffsets;
    private final KafkaFetchClientDecoder decodeOffsetsTopics = this::decodeOffsetsTopics;
    private final KafkaFetchClientDecoder decodeOffsetsTopic = this::decodeOffsetsTopic;
    private final KafkaFetchClientDecoder decodeOffsetsPartitions = this::decodeOffsetsPartitions;
    private final KafkaFetchClientDecoder decodeOffsetsPartition = this::decodeOffsetsPartition;
    private final KafkaFetchClientDecoder decodeFetchResponse = this::decodeFetchResponse;
    private final KafkaFetchClientDecoder decodeFetch = this::decodeFetch;
    private final KafkaFetchClientDecoder decodeFetchTopic = this::decodeFetchTopic;
    private final KafkaFetchClientDecoder decodeFetchPartition = this::decodeFetchPartition;
    private final KafkaFetchClientDecoder decodeFetchTransaction = this::decodeFetchTransaction;
    private final KafkaFetchClientDecoder decodeFetchRecordSet = this::decodeFetchRecordSet;
    private final KafkaFetchClientDecoder decodeFetchRecordBatch = this::decodeFetchRecordBatch;
    private final KafkaFetchClientDecoder decodeFetchRecordLength = this::decodeFetchRecordLength;
    private final KafkaFetchClientDecoder decodeFetchRecord = this::decodeFetchRecord;
    private final KafkaFetchClientDecoder decodeFetchRecordInit = this::decodeFetchRecordInit;
    private final KafkaFetchClientDecoder decodeFetchRecordValue = this::decodeFetchRecordValue;
    private final KafkaFetchClientDecoder decodeIgnoreRecord = this::decodeIgnoreRecord;
    private final KafkaFetchClientDecoder decodeIgnoreRecordBatch = this::decodeIgnoreRecordBatch;
    private final KafkaFetchClientDecoder decodeIgnoreRecordSet = this::decodeIgnoreRecordSet;
    private final KafkaFetchClientDecoder decodeIgnoreAll = this::decodeIgnoreAll;

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final int fetchMaxBytes;
    private final int fetchMaxWaitMillis;
    private final int partitionMaxBytes;
    private final int kafkaTypeId;
    private final int proxyTypeId;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final BufferPool decodePool;
    private final BufferPool encodePool;
    private final Signaler signaler;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongFunction<BudgetDebitor> supplyDebitor;
    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final LongFunction<KafkaClientRoute> supplyClientRoute;
    private final int decodeMaxBytes;

    public KafkaClientFetchFactory(
        KafkaConfiguration config,
        RouteManager router,
        Signaler signaler,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        LongFunction<BudgetDebitor> supplyDebitor,
        Long2ObjectHashMap<MessageConsumer> correlations,
        LongFunction<KafkaClientRoute> supplyClientRoute)
    {
        this.fetchMaxBytes = config.clientFetchMaxBytes();
        this.fetchMaxWaitMillis = config.clientFetchMaxWaitMillis();
        this.partitionMaxBytes = config.clientFetchPartitionMaxBytes();
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.proxyTypeId = supplyTypeId.applyAsInt("proxy");
        this.router = router;
        this.signaler = signaler;
        this.writeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.decodePool = bufferPool;
        this.encodePool = bufferPool;
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.supplyDebitor = supplyDebitor;
        this.correlations = correlations;
        this.supplyClientRoute = supplyClientRoute;
        this.decodeMaxBytes = decodePool.slotCapacity();
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
        final long routeId = begin.routeId();
        final long initialId = begin.streamId();
        final long leaderId = begin.affinity();
        final long authorization = begin.authorization();
        final OctetsFW extension = begin.extension();
        final ExtensionFW beginEx = extensionRO.tryWrap(extension.buffer(), extension.offset(), extension.limit());
        final KafkaBeginExFW kafkaBeginEx = beginEx.typeId() == kafkaTypeId ? extension.get(kafkaBeginExRO::wrap) : null;
        assert kafkaBeginEx == null || kafkaBeginEx.kind() == KafkaBeginExFW.KIND_FETCH;
        final KafkaFetchBeginExFW kafkaFetchBeginEx = kafkaBeginEx != null ? kafkaBeginEx.fetch() : null;

        MessageConsumer newStream = null;

        if (beginEx != null && kafkaFetchBeginEx != null &&
            kafkaFetchBeginEx.filters().isEmpty())
        {
            final String16FW beginTopic = kafkaFetchBeginEx.topic();

            final MessagePredicate filter = (t, b, i, l) ->
            {
                final RouteFW route = wrapRoute.apply(t, b, i, l);
                final KafkaRouteExFW routeEx = route.extension().get(kafkaRouteExRO::tryWrap);
                final String16FW routeTopic = routeEx != null ? routeEx.topic() : null;
                final KafkaDeltaType routeDeltaType = routeEx != null ? routeEx.deltaType().get() : DEFAULT_DELTA_TYPE;
                return !route.localAddress().equals(route.remoteAddress()) &&
                        (beginTopic != null && (routeTopic == null || routeTopic.equals(beginTopic))) &&
                        routeDeltaType == KafkaDeltaType.NONE;
            };

            final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);

            if (route != null)
            {
                final long resolvedId = route.correlationId();
                final String topic = beginTopic != null ? beginTopic.asString() : null;
                final KafkaOffsetFW partition = kafkaFetchBeginEx.partition();
                final int partitionId = partition.partitionId();
                final long initialOffset = partition.partitionOffset();
                final long latestOffset = partition.latestOffset();

                newStream = new KafkaFetchStream(
                    application,
                    routeId,
                    initialId,
                    resolvedId,
                    topic,
                    partitionId,
                    latestOffset,
                    leaderId,
                    initialOffset)::onApplication;
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

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long affinity,
        Consumer<OctetsFW.Builder> extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .extension(extension)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer payload,
        int offset,
        int length,
        Flyweight extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload, offset, length)
                .extension(extension.buffer(), extension.offset(), extension.sizeof())
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        int flags,
        long budgetId,
        int reserved,
        OctetsFW payload,
        Flyweight extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .flags(flags)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload)
                .extension(extension.buffer(), extension.offset(), extension.sizeof())
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Consumer<OctetsFW.Builder> extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .routeId(routeId)
                               .streamId(streamId)
                               .traceId(traceId)
                               .authorization(authorization)
                               .extension(extension)
                               .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Consumer<OctetsFW.Builder> extension)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .extension(extension)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doFlush(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Consumer<OctetsFW.Builder> extension)
    {
        final FlushFW flush = flushRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(0L)
                .extension(extension)
                .build();

        receiver.accept(flush.typeId(), flush.buffer(), flush.offset(), flush.sizeof());
    }

    private void doWindow(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long budgetId,
        int credit,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .credit(credit)
                .padding(padding)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .traceId(traceId)
               .authorization(authorization)
               .extension(extension.buffer(), extension.offset(), extension.sizeof())
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    @FunctionalInterface
    private interface KafkaFetchClientDecoder
    {
        int decode(
            KafkaFetchStream.KafkaFetchClient client,
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            MutableDirectBuffer buffer,
            int offset,
            int progress,
            int limit);
    }

    private int decodeOffsetsResponse(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        if (length != 0)
        {
            final ResponseHeaderFW responseHeader = responseHeaderRO.tryWrap(buffer, progress, limit);
            if (responseHeader != null)
            {
                progress = responseHeader.limit();
                client.decodableResponseBytes = responseHeader.length();
                client.decoder = decodeOffsets;
            }
        }

        return progress;
    }

    private int decodeOffsets(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        if (length != 0)
        {
            final OffsetsResponseFW offsetsResponse = offsetsResponseRO.tryWrap(buffer, progress, limit);

            if (offsetsResponse != null)
            {
                progress = offsetsResponse.limit();

                client.decodableResponseBytes -= offsetsResponse.sizeof();
                assert client.decodableResponseBytes >= 0;

                client.decodableTopics = offsetsResponse.topicCount();
                client.decoder = decodeOffsetsTopics;
            }
        }

        return progress;
    }

    private int decodeOffsetsTopics(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        if (client.decodableTopics == 0)
        {
            client.onDecodeResponse(traceId);
            client.encoder = client.encodeFetchRequest;
            client.decoder = decodeFetchResponse;
        }
        else
        {
            client.decoder = decodeOffsetsTopic;
        }

        return progress;
    }

    private int decodeOffsetsTopic(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (length != 0)
        {
            final OffsetsTopicResponseFW topic = offsetsTopicResponseRO.tryWrap(buffer, progress, limit);
            if (topic == null)
            {
                break decode;
            }

            final String topicName = topic.name().asString();
            assert client.topic.equals(topicName);

            progress = topic.limit();

            client.decodableResponseBytes -= topic.sizeof();
            assert client.decodableResponseBytes >= 0;

            client.decodablePartitions = topic.partitionCount();
            client.decoder = decodeOffsetsPartitions;
        }

        return progress;
    }

    private int decodeOffsetsPartitions(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        if (client.decodablePartitions == 0)
        {
            client.decodableTopics--;
            assert client.decodableTopics >= 0;

            client.decoder = decodeOffsetsTopics;
        }
        else
        {
            client.decoder = decodeOffsetsPartition;
        }

        return progress;
    }

    private int decodeOffsetsPartition(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (length != 0)
        {
            final OffsetsPartitionResponseFW partition = offsetsPartitionResponseRO.tryWrap(buffer, progress, limit);
            if (partition == null)
            {
                break decode;
            }

            final int partitionId = partition.partitionId();
            final int errorCode = partition.errorCode();
            final long partitionOffset = partition.offset$();

            progress = partition.limit();

            client.decodableResponseBytes -= partition.sizeof();
            assert client.decodableResponseBytes >= 0;

            client.decodablePartitions--;
            assert client.decodablePartitions >= 0;

            client.onDecodeOffsetsPartition(traceId, authorization, errorCode, partitionId, partitionOffset);

            client.decoder = decodeOffsetsPartitions;
        }

        return progress;
    }

    private int decodeFetchResponse(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        if (length != 0)
        {
            final ResponseHeaderFW responseHeader = responseHeaderRO.tryWrap(buffer, progress, limit);
            if (responseHeader != null)
            {
                progress = responseHeader.limit();
                client.decodableResponseBytes = responseHeader.length();
                client.decoder = decodeFetch;
            }
        }

        return progress;
    }

    private int decodeFetch(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        if (length != 0)
        {
            final FetchResponseFW fetchResponse = fetchResponseRO.tryWrap(buffer, progress, limit);

            if (fetchResponse != null)
            {
                progress = fetchResponse.limit();

                client.decodableTopics = fetchResponse.topicCount();
                client.decodableResponseBytes -= fetchResponse.sizeof();
                assert client.decodableResponseBytes >= 0;
                client.decoder = decodeFetchTopic;
            }
        }

        return progress;
    }

    private int decodeFetchTopic(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (client.decodableTopics == 0)
        {
            client.onDecodeResponse(traceId);
            client.decoder = decodeFetchResponse;
            break decode;
        }
        else if (length != 0)
        {
            final TopicResponseFW topic = topicResponseRO.tryWrap(buffer, progress, limit);
            if (topic != null)
            {
                final String topicName = topic.name().asString();
                assert client.topic.equals(topicName);

                progress = topic.limit();

                client.decodableResponseBytes -= topic.sizeof();
                assert client.decodableResponseBytes >= 0;

                client.decodablePartitions = topic.partitionCount();
                client.decoder = decodeFetchPartition;
            }
        }

        return progress;
    }

    private int decodeFetchPartition(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (client.decodablePartitions == 0)
        {
            client.decodableTopics--;
            assert client.decodableTopics >= 0;
            client.decoder = decodeFetchTopic;
            break decode;
        }
        else if (length != 0)
        {
            final PartitionResponseFW partition = partitionResponseRO.tryWrap(buffer, progress, limit);
            if (partition != null)
            {
                final int partitionId = partition.partitionId();
                final int errorCode = partition.errorCode();

                client.latestOffset = partition.highWatermark() - 1;

                client.decodePartitionError = errorCode;
                client.decodePartitionId = partitionId;
                client.decodableTransactions = partition.abortedTransactionCount();

                progress = partition.limit();

                client.decodableResponseBytes -= partition.sizeof();
                assert client.decodableResponseBytes >= 0;

                client.onDecodeFetchPartition(traceId, authorization, partitionId, errorCode);
                client.decoder = decodeFetchTransaction;
            }
        }

        return progress;
    }

    private int decodeFetchTransaction(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (client.decodableTransactions <= 0)
        {
            client.decoder = decodeFetchRecordSet;
            break decode;
        }
        else if (length != 0)
        {
            final TransactionResponseFW transaction = transactionResponseRO.tryWrap(buffer, progress, limit);
            if (transaction != null)
            {
                progress = transaction.limit();

                client.decodableResponseBytes -= transaction.sizeof();
                assert client.decodableResponseBytes >= 0;
                client.decodableTransactions--;
                assert client.decodableTransactions >= 0;
            }
        }

        return progress;
    }

    private int decodeFetchRecordSet(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        if (length != 0)
        {
            final RecordSetFW recordSet = recordSetRO.tryWrap(buffer, progress, limit);
            if (recordSet != null)
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH RecordSet %d\n",
                            client.replyId, client.topic, client.partitionId, recordSet.length());
                }

                final int responseProgress = recordSet.sizeof();

                progress += responseProgress;

                client.decodableResponseBytes -= responseProgress;
                assert client.decodableResponseBytes >= 0;

                client.decodableRecordSetBytes = recordSet.length();
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record Set Bytes %d\n",
                        client.replyId, client.topic, client.partitionId, client.decodableRecordSetBytes);
                }
                assert client.decodableRecordSetBytes >= 0 : "negative recordSetSize";
                assert client.decodableRecordSetBytes <= client.decodableResponseBytes : "record set overflows response";

                if (client.decodePartitionError != 0 || client.decodableRecordSetBytes == 0)
                {
                    client.decoder = decodeIgnoreRecordSet;
                }
                else
                {
                    client.decoder = decodeFetchRecordBatch;
                }
            }
        }

        return progress;
    }

    private int decodeFetchRecordBatch(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (client.decodableRecordSetBytes == 0)
        {
            client.decodablePartitions--;
            assert client.decodablePartitions >= 0;
            client.decoder = decodeFetchPartition;
            break decode;
        }
        else if (length != 0)
        {
            final int maxLimit = Math.min(progress + client.decodableRecordSetBytes, limit);
            final RecordBatchFW recordBatch = recordBatchRO.tryWrap(buffer, progress, maxLimit);

            if (recordBatch == null && length >= client.decodableRecordSetBytes)
            {
                client.decoder = decodeIgnoreRecordSet;
                break decode;
            }

            if (recordBatch != null)
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH RecordBatch %d %d %d\n",
                            client.replyId, client.topic, client.partitionId, recordBatch.baseOffset(),
                        recordBatch.lastOffsetDelta(), recordBatch.length());
                }

                final int attributes = recordBatch.attributes();
                final int recordSetProgress = recordBatch.sizeof();
                final int recordBatchProgress = recordSetProgress - FIELD_LIMIT_RECORD_BATCH_LENGTH;

                progress += recordSetProgress;

                client.decodableRecordBatchBytes = recordBatch.length();
                client.decodeRecordBatchOffset = recordBatch.baseOffset();
                client.decodeRecordBatchLastOffset = recordBatch.baseOffset() + recordBatch.lastOffsetDelta();
                client.decodeRecordBatchTimestamp = recordBatch.firstTimestamp();
                client.decodableRecords = recordBatch.recordCount();

                client.decodableResponseBytes -= recordSetProgress;
                assert client.decodableResponseBytes >= 0;

                client.decodableRecordSetBytes -= recordSetProgress;
                assert client.decodableRecordSetBytes >= 0;

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record Set Bytes %d\n",
                        client.replyId, client.topic, client.partitionId, client.decodableRecordSetBytes);
                }

                client.decodableRecordBatchBytes -= recordBatchProgress;
                assert client.decodableRecordBatchBytes >= 0;

                if (isCompressedBatch(attributes) || isControlBatch(attributes))
                {
                    client.decoder = decodeIgnoreRecordBatch;
                    break decode;
                }

                client.decoder = decodeFetchRecordLength;
            }
        }

        return progress;
    }

    private int decodeFetchRecordLength(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (client.decodableRecords == 0)
        {
            client.nextOffset = Math.max(client.nextOffset, client.decodeRecordBatchLastOffset + 1);
            client.decoder = decodeFetchRecordBatch;
            break decode;
        }
        else if (length != 0)
        {
            final Varint32FW recordLength = recordLengthRO.tryWrap(buffer, progress, limit);

            // TODO: verify incremental decode vs truncated record set mid record length
            if (recordLength == null && length >= client.decodableRecordBatchBytes)
            {
                client.decoder = decodeIgnoreRecordBatch;
                break decode;
            }

            if (recordLength != null)
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record length %d\n",
                            client.replyId, client.topic, client.partitionId, recordLength.value());
                }

                final int sizeofRecord = recordLength.sizeof() + recordLength.value();

                // TODO: verify incremental decode vs truncated record set post record length
                if (sizeofRecord > client.decodableRecordSetBytes)
                {
                    client.decoder = decodeIgnoreRecordSet;
                    break decode;
                }

                assert sizeofRecord <= client.decodableRecordBatchBytes : "truncated record batch not last in record set";

                if (length >= sizeofRecord)
                {
                    client.decoder = decodeFetchRecord;
                }
                else if (sizeofRecord > decodePool.slotCapacity())
                {
                    client.decoder = decodeFetchRecordInit;
                }
            }
        }

        return progress;
    }

    private int decodeFetchRecord(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (length != 0)
        {
            final RecordHeaderFW recordHeader = recordHeaderRO.tryWrap(buffer, progress, limit);
            if (recordHeader != null)
            {
                final Varint32FW recordLength = recordLengthRO.tryWrap(buffer, progress, limit);
                assert recordHeader.length() == recordLength.value();

                final int sizeofRecord = recordLength.sizeof() + recordLength.value();
                final int recordLimit = recordHeader.offset() + sizeofRecord;

                final long offsetAbs = client.decodeRecordBatchOffset + recordHeader.offsetDelta();
                final long timestampAbs = client.decodeRecordBatchTimestamp + recordHeader.timestampDelta();
                final OctetsFW key = recordHeader.key();

                client.decodeRecordOffset = offsetAbs;

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record %d\n",
                        client.replyId, client.topic, client.partitionId, client.decodeRecordOffset);
                }

                if (offsetAbs < client.nextOffset)
                {
                    client.decodableRecordBytes = sizeofRecord;
                    client.decoder = decodeIgnoreRecord;
                    break decode;
                }

                final int valueLength = recordHeader.valueLength();
                final int valueOffset = recordHeader.limit();
                final int valueSize = Math.max(valueLength, 0);
                final int valueReserved = valueSize + client.stream.replyPadding;

                if (sizeofRecord <= length && valueReserved <= client.stream.replyBudget)
                {
                    final int maximum = valueReserved;
                    final int minimum = Math.min(maximum, 1024);

                    int valueClaimed = maximum;
                    if (valueClaimed != 0 && client.stream.replyDebitorIndex != NO_DEBITOR_INDEX)
                    {
                        valueClaimed = client.stream.replyDebitor.claim(traceId, client.stream.replyDebitorIndex,
                                client.stream.replyId, minimum, maximum, 0);

                        if (valueClaimed == 0)
                        {
                            break decode;
                        }
                    }

                    if (valueClaimed < valueReserved)
                    {
                        final int valueLengthMax = valueClaimed - client.stream.replyBudget;
                        final int limitMax = valueOffset + valueLengthMax;

                        // TODO: shared budget for fragmented decode too
                        progress = decodeFetchRecordInit(client, traceId, authorization, budgetId, valueReserved,
                                buffer, offset, progress, limitMax);
                    }
                    else
                    {
                        final OctetsFW value =
                                valueLength != -1 ? valueRO.wrap(buffer, valueOffset, valueOffset + valueLength) : null;

                        final int trailerOffset = valueOffset + Math.max(valueLength, 0);
                        final RecordTrailerFW recordTrailer = recordTrailerRO.wrap(buffer, trailerOffset, recordLimit);
                        final int headerCount = recordTrailer.headerCount();
                        final int headersOffset = recordTrailer.limit();
                        final int headersLength = recordLimit - headersOffset;
                        final DirectBuffer headers = wrapHeaders(buffer, headersOffset, headersLength);

                        progress += sizeofRecord;

                        client.onDecodeFetchRecord(traceId, valueReserved, offsetAbs, timestampAbs,
                                key, value, headerCount, headers);

                        client.decodableResponseBytes -= sizeofRecord;
                        assert client.decodableResponseBytes >= 0;

                        client.decodableRecordSetBytes -= sizeofRecord;
                        assert client.decodableRecordSetBytes >= 0;

                        if (KafkaConfiguration.DEBUG)
                        {
                            System.out.format("[client] [0x%016x] %s[%d] FETCH Record Set Bytes %d\n",
                                client.replyId, client.topic, client.partitionId, client.decodableRecordSetBytes);
                        }

                        client.decodableRecordBatchBytes -= sizeofRecord;
                        assert client.decodableRecordBatchBytes >= 0;

                        client.decodableRecords--;
                        assert client.decodableRecords >= 0;

                        client.decoder = decodeFetchRecordLength;
                    }
                }
            }
        }

        if (client.decoder == decodeIgnoreAll)
        {
            client.cleanupNetwork(traceId);
        }

        return progress;
    }

    private int decodeFetchRecordInit(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (length >= decodeMaxBytes)
        {
            final RecordHeaderFW recordHeader = recordHeaderRO.tryWrap(buffer, progress, limit);
            if (recordHeader != null)
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record (init) %d\n",
                            client.replyId, client.topic, client.partitionId, client.nextOffset);
                }

                final Varint32FW recordLength = recordLengthRO.tryWrap(buffer, progress, limit);
                assert recordHeader.length() == recordLength.value();

                final int sizeofRecord = recordLength.sizeof() + recordLength.value();
                final long offsetAbs = client.decodeRecordBatchOffset + recordHeader.offsetDelta();
                final long timestampAbs = client.decodeRecordBatchTimestamp + recordHeader.timestampDelta();
                final OctetsFW key = recordHeader.key();

                if (offsetAbs < client.nextOffset)
                {
                    client.decodableRecordBytes = sizeofRecord;
                    client.decoder = decodeIgnoreRecord;
                    break decode;
                }

                final int valueLength = recordHeader.valueLength();
                final int valueOffset = recordHeader.limit();

                final int valueSize = Math.max(valueLength, 0);
                final int valueReserved = valueSize + client.stream.replyPadding;
                final int valueReservedMax = Math.min(valueReserved, client.stream.replyBudget);

                final int maximum = valueReservedMax;
                final int minimum = Math.min(maximum, 1024);

                int valueClaimed = maximum;
                if (valueClaimed != 0 && client.stream.replyDebitorIndex != NO_DEBITOR_INDEX)
                {
                    valueClaimed = client.stream.replyDebitor.claim(traceId, client.stream.replyDebitorIndex,
                            client.stream.replyId, minimum, maximum, 0);

                    if (valueClaimed == 0)
                    {
                        break decode;
                    }
                }

                final int valueLengthMax = valueClaimed - client.stream.replyPadding;
                final int valueLimitMax = Math.min(limit, valueOffset + valueLengthMax);
                final OctetsFW valueInit = valueLength != -1 ? valueRO.wrap(buffer, valueOffset, valueLimitMax) : null;
                final int valueProgress = valueInit != null ? valueInit.sizeof() : 0;
                final int recordProgress = recordHeader.sizeof() + valueProgress;

                final int valueDeferred = valueLength - valueProgress;
                final int headersLength = recordHeader.length() - (recordHeader.limit() - recordLength.limit()) - valueLength;
                final int headersSizeMax = headersLength + 3;

                progress += recordProgress;

                client.onDecodeFetchRecordValueInit(traceId, valueReservedMax, valueDeferred, offsetAbs,
                        timestampAbs, headersSizeMax, key, valueInit);

                client.decodeRecordOffset = offsetAbs;
                client.decodableRecordValueBytes = valueLength != -1 ? valueLength - valueProgress : -1;
                client.decodableRecordBytes = sizeofRecord;

                client.decodableResponseBytes -= recordProgress;
                assert client.decodableResponseBytes >= 0;

                client.decodableRecordSetBytes -= recordProgress;
                assert client.decodableRecordSetBytes >= 0;

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record Set Bytes %d\n",
                        client.replyId, client.topic, client.partitionId, client.decodableRecordSetBytes);
                }

                client.decodableRecordBatchBytes -= recordProgress;
                assert client.decodableRecordBatchBytes >= 0;

                client.decodableRecordBytes -= recordProgress;
                assert client.decodableRecordBytes >= 0;

                client.decoder = decodeFetchRecordValue;
            }
        }

        return progress;
    }

    private int decodeFetchRecordValue(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int length = limit - progress;

        decode:
        if (length >= Math.min(decodeMaxBytes, client.decodableRecordBytes))
        {
            final int valueReserved = Math.min(length, client.decodableRecordValueBytes) + client.stream.replyPadding;
            final int valueReservedMax = Math.min(valueReserved, client.stream.replyBudget);

            final int maximum = valueReservedMax;
            final int minimum = Math.min(maximum, 1024);

            int valueClaimed = maximum;
            if (valueClaimed != 0 && client.stream.replyDebitorIndex != NO_DEBITOR_INDEX)
            {
                valueClaimed = client.stream.replyDebitor.claim(traceId, client.stream.replyDebitorIndex,
                        client.stream.replyId, minimum, maximum, 0);

                if (valueClaimed == 0)
                {
                    break decode;
                }
            }

            final int valueLengthMax = valueClaimed - client.stream.replyPadding;

            if (length >= client.decodableRecordBytes && valueLengthMax >= client.decodableRecordValueBytes)
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record (fin) %d\n",
                            client.replyId, client.topic, client.partitionId, client.nextOffset);
                }

                final int valueOffset = progress;
                final int valueProgress = valueLengthMax;
                final int valueSize = Math.max(valueProgress, 0);
                final int valueLimit = valueOffset + valueSize;
                final OctetsFW valueFin = valueProgress != -1 ? valueRO.wrap(buffer, valueOffset, valueLimit) : null;

                final int recordProgress = client.decodableRecordBytes;
                final int recordLimit = progress + recordProgress;
                final RecordTrailerFW recordTrailer = recordTrailerRO.wrap(buffer, valueLimit, recordLimit);
                final int headerCount = recordTrailer.headerCount();
                final int headersOffset = recordTrailer.limit();
                final int headersLength = recordLimit - headersOffset;
                final DirectBuffer headers = headersRO;
                headers.wrap(buffer, headersOffset, headersLength);

                progress += recordProgress;

                client.onDecodeFetchRecordValueFin(traceId, valueReservedMax, client.decodeRecordOffset, valueFin,
                        headerCount, headers);

                client.decodableResponseBytes -= recordProgress;
                assert client.decodableResponseBytes >= 0;

                client.decodableRecordSetBytes -= recordProgress;
                assert client.decodableRecordSetBytes >= 0;

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record Set Bytes %d\n",
                        client.replyId, client.topic, client.partitionId, client.decodableRecordSetBytes);
                }

                client.decodableRecordBatchBytes -= recordProgress;
                assert client.decodableRecordBatchBytes >= 0;

                client.decodableRecordBytes -= recordProgress;
                assert client.decodableRecordBytes == 0;

                client.decodableRecordValueBytes -= valueProgress;
                assert client.decodableRecordValueBytes == 0;

                client.decodableRecords--;
                assert client.decodableRecords >= 0;

                client.decoder = decodeFetchRecordLength;
            }
            else
            {
                assert client.decodableRecordValueBytes != -1 : "record headers overflow decode buffer";

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record (cont) %d\n",
                            client.replyId, client.topic, client.partitionId, client.nextOffset);
                }

                final int valueOffset = progress;
                final int valueProgress = Math.min(valueLengthMax, decodeMaxBytes);
                final int valueLimit = valueOffset + valueProgress;
                final OctetsFW value = valueRO.wrap(buffer, valueOffset, valueLimit);

                progress += valueProgress;

                client.onDecodeFetchRecordValueCont(traceId, valueReservedMax, value);

                client.decodableRecordBytes -= valueProgress;
                assert client.decodableRecordBytes >= 0;

                client.decodableRecordSetBytes -= valueProgress;
                assert client.decodableRecordSetBytes >= 0;

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH Record Set Bytes %d\n",
                        client.replyId, client.topic, client.partitionId, client.decodableRecordSetBytes);
                }

                client.decodableRecordValueBytes -= valueProgress;
                assert client.decodableRecordValueBytes >= 0;
            }
        }

        return progress;
    }

    private int decodeIgnoreRecord(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int maxLength = limit - progress;
        final int length = Math.min(maxLength, client.decodableRecordBytes);

        progress += length;

        client.decodableResponseBytes -= length;
        assert client.decodableResponseBytes >= 0;

        client.decodableRecordSetBytes -= length;
        assert client.decodableRecordSetBytes >= 0;

        if (KafkaConfiguration.DEBUG)
        {
            System.out.format("[client] [0x%016x] %s[%d] FETCH Record Set Bytes %d\n",
                client.replyId, client.topic, client.partitionId, client.decodableRecordSetBytes);
        }

        client.decodableRecordBatchBytes -= length;
        assert client.decodableRecordBatchBytes >= 0;

        client.decodableRecordBytes -= length;
        assert client.decodableRecordBytes >= 0;

        if (client.decodableRecordBytes == 0)
        {
            client.decodableRecords--;
            assert client.decodableRecords >= 0;
            client.decoder = decodeFetchRecordLength;
        }

        return progress;
    }

    private int decodeIgnoreRecordBatch(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int maxLength = limit - progress;
        final int length = Math.min(maxLength, client.decodableRecordBatchBytes);

        progress += length;

        client.decodableResponseBytes -= length;
        assert client.decodableResponseBytes >= 0;

        client.decodableRecordSetBytes -= length;
        assert client.decodableRecordSetBytes >= 0;

        if (KafkaConfiguration.DEBUG)
        {
            System.out.format("[client] [0x%016x] %s[%d] FETCH Record Set Bytes %d\n",
                client.replyId, client.topic, client.partitionId, client.decodableRecordSetBytes);
        }

        client.decodableRecordBatchBytes -= length;
        assert client.decodableRecordBatchBytes >= 0;

        if (client.decodableRecordBatchBytes == 0)
        {
            client.decoder = decodeFetchRecordBatch;
        }

        return progress;
    }

    private int decodeIgnoreRecordSet(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        final int maxLength = limit - progress;
        final int length = Math.min(maxLength, client.decodableRecordSetBytes);

        progress += length;

        client.decodableResponseBytes -= length;
        assert client.decodableResponseBytes >= 0;

        client.decodableRecordSetBytes -= length;
        assert client.decodableRecordSetBytes >= 0;

        if (KafkaConfiguration.DEBUG)
        {
            System.out.format("[client] [0x%016x] %s[%d] FETCH Record Set Bytes %d\n",
                client.replyId, client.topic, client.partitionId, client.decodableRecordSetBytes);
        }

        if (client.decodableRecordSetBytes == 0)
        {
            client.decodablePartitions--;
            assert client.decodablePartitions >= 0;
            client.decoder = decodeFetchPartition;
        }

        return progress;
    }

    private int decodeIgnoreAll(
        KafkaFetchStream.KafkaFetchClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int progress,
        int limit)
    {
        return limit;
    }

    private final class KafkaFetchStream
    {
        private final MessageConsumer application;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long leaderId;
        private final KafkaClientRoute clientRoute;
        private final KafkaFetchClient client;

        private int state;

        private int replyBudget;
        private int replyPadding;
        private long replyDebitorId;
        private BudgetDebitor replyDebitor;
        private long replyDebitorIndex = NO_DEBITOR_INDEX;

        KafkaFetchStream(
            MessageConsumer application,
            long routeId,
            long initialId,
            long resolvedId,
            String topic,
            int partitionId,
            long latestOffset,
            long leaderId,
            long initialOffset)
        {
            this.application = application;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.leaderId = leaderId;
            this.clientRoute = supplyClientRoute.apply(resolvedId);
            this.client = new KafkaFetchClient(resolvedId, topic, partitionId, initialOffset, latestOffset);
        }

        private void onApplication(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onApplicationBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onApplicationData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onApplicationEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onApplicationAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onApplicationWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onApplicationReset(reset);
                break;
            default:
                break;
            }
        }

        private void onApplicationBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();

            state = KafkaState.openingInitial(state);

            if (clientRoute.partitions.get(client.partitionId) != leaderId)
            {
                cleanupApplication(traceId, ERROR_NOT_LEADER_FOR_PARTITION);
            }
            else
            {
                client.doNetworkBegin(traceId, authorization, leaderId);
            }
        }

        private void onApplicationData(
            DataFW data)
        {
            final long traceId = data.traceId();

            client.cleanupNetwork(traceId);
        }

        private void onApplicationEnd(
            EndFW end)
        {
            final long traceId = end.traceId();
            final long authorization = end.authorization();

            state = KafkaState.closedInitial(state);

            client.doNetworkEndAfterFlush(traceId, authorization);
        }

        private void onApplicationAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedInitial(state);

            client.doNetworkAbortIfNecessary(traceId);
        }

        private void onApplicationWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            replyDebitorId = budgetId;
            replyBudget += credit;
            replyPadding = padding;

            if (replyDebitorId != 0L && replyDebitor == null)
            {
                replyDebitor = supplyDebitor.apply(replyDebitorId);
                replyDebitorIndex = replyDebitor.acquire(replyDebitorId, replyId, client::decodeNetworkIfNecessary);
            }

            state = KafkaState.openedReply(state);

            client.decodeNetworkIfNecessary(traceId);
        }

        private void onApplicationReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedInitial(state);

            client.doNetworkResetIfNecessary(traceId);
        }

        private void doApplicationBeginIfNecessary(
            long traceId,
            long authorization,
            String topic,
            int partitionId,
            long partitionOffset,
            long latestOffset)
        {
            if (!KafkaState.replyOpening(state))
            {
                doApplicationBegin(traceId, authorization, topic, partitionId, partitionOffset, latestOffset);
            }
        }

        private void doApplicationBegin(
            long traceId,
            long authorization,
            String topic,
            int partitionId,
            long partitionOffset,
            long latestOffset)
        {
            state = KafkaState.openingReply(state);

            router.setThrottle(replyId, this::onApplication);
            doBegin(application, routeId, replyId, traceId, authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                                                        .typeId(kafkaTypeId)
                                                        .fetch(m -> m.topic(topic)
                                                                     .partition(p -> p.partitionId(partitionId)
                                                                                      .partitionOffset(partitionOffset)
                                                                                      .latestOffset(latestOffset)))
                                                        .build()
                                                        .sizeof()));
        }

        private void doApplicationData(
            long traceId,
            long authorization,
            int flags,
            int reserved,
            OctetsFW payload,
            Flyweight extension)
        {
            replyBudget -= reserved;

            assert replyBudget >= 0;

            doData(application, routeId, replyId, traceId, authorization, flags, replyDebitorId, reserved, payload, extension);
        }

        private void doApplicationEnd(
            long traceId)
        {
            cleanupApplicationDebitorIfNecessary();

            state = KafkaState.closedReply(state);
            //client.stream = nullIfClosed(state, client.stream);
            doEnd(application, routeId, replyId, traceId, client.authorization, EMPTY_EXTENSION);
        }

        private void doApplicationAbort(
            long traceId)
        {
            cleanupApplicationDebitorIfNecessary();

            state = KafkaState.closedReply(state);
            //client.stream = nullIfClosed(state, client.stream);
            doAbort(application, routeId, replyId, traceId, client.authorization, EMPTY_EXTENSION);
        }

        private void doApplicationWindow(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            if (!KafkaState.initialOpened(state) || credit > 0)
            {
                doWindow(application, routeId, initialId, traceId, client.authorization,
                        budgetId, credit, padding);
            }

            state = KafkaState.openedInitial(state);
        }

        private void doApplicationReset(
            long traceId,
            Flyweight extension)
        {
            state = KafkaState.closedInitial(state);
            //client.stream = nullIfClosed(state, client.stream);

            doReset(application, routeId, initialId, traceId, client.authorization, extension);
        }

        private void doApplicationAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doApplicationAbort(traceId);
            }
        }

        private void doApplicationResetIfNecessary(
            long traceId,
            Flyweight extension)
        {
            if (!KafkaState.initialClosed(state))
            {
                doApplicationReset(traceId, extension);
            }
        }

        private void cleanupApplication(
            long traceId,
            int error)
        {
            final KafkaResetExFW kafkaResetEx = kafkaResetExRW.wrap(extBuffer, 0, extBuffer.capacity())
                    .typeId(kafkaTypeId)
                    .error(error)
                    .build();

            cleanupApplication(traceId, kafkaResetEx);
        }

        private void cleanupApplication(
            long traceId,
            Flyweight extension)
        {
            doApplicationResetIfNecessary(traceId, extension);
            doApplicationAbortIfNecessary(traceId);
        }

        private void cleanupApplicationDebitorIfNecessary()
        {
            if (replyDebitorIndex != NO_DEBITOR_INDEX)
            {
                replyDebitor.release(replyDebitorIndex, replyId);
                replyDebitorIndex = NO_DEBITOR_INDEX;
                replyDebitor = null;
            }
        }

        private final class KafkaFetchClient
        {
            private final LongLongConsumer encodeOffsetsRequest = this::doEncodeOffsetsRequest;
            private final LongLongConsumer encodeFetchRequest = this::doEncodeFetchRequest;

            private final KafkaFetchStream stream;
            private final long routeId;
            private final long initialId;
            private final long replyId;
            private final MessageConsumer network;
            private final String topic;
            private final int partitionId;

            private long nextOffset;
            private long latestOffset;

            private int state;
            private long authorization;

            private long initialBudgetId;
            private int initialBudget;
            private int initialPadding;
            private int replyBudget;

            private int encodeSlot = NO_SLOT;
            private int encodeSlotOffset;
            private long encodeSlotTraceId;

            private int decodeSlot = NO_SLOT;
            private int decodeSlotOffset;
            private int decodeSlotReserved;

            private int decodableResponseBytes;
            private int decodableTopics;
            private int decodableTransactions;
            private int decodablePartitions;
            private int decodePartitionError;
            private int decodePartitionId;
            private int decodableRecordSetBytes;
            private int decodableRecordBatchBytes;
            private long decodeRecordBatchOffset;
            private long decodeRecordBatchLastOffset;
            private long decodeRecordBatchTimestamp;
            private int decodableRecords;
            private long decodeRecordOffset;
            private int decodableRecordBytes;
            private int decodableRecordValueBytes;

            private int nextRequestId;
            private int nextResponseId;

            private KafkaFetchClientDecoder decoder;
            private LongLongConsumer encoder;

            KafkaFetchClient(
                long routeId,
                String topic,
                int partitionId,
                long initialOffset,
                long latestOffset)
            {
                this.stream = KafkaFetchStream.this;
                this.routeId = routeId;
                this.initialId = supplyInitialId.applyAsLong(routeId);
                this.replyId = supplyReplyId.applyAsLong(initialId);
                this.network = router.supplyReceiver(initialId);
                this.topic = requireNonNull(topic);
                this.partitionId = partitionId;
                this.nextOffset = initialOffset;
                this.latestOffset = latestOffset;
                this.encoder = encodeFetchRequest;
                this.decoder = decodeFetchResponse;
            }

            private void onNetwork(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onNetworkBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onNetworkData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onNetworkEnd(end);
                    break;
                case AbortFW.TYPE_ID:
                    final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                    onNetworkAbort(abort);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    onNetworkReset(reset);
                    break;
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onNetworkWindow(window);
                    break;
                case SignalFW.TYPE_ID:
                    final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                    onNetworkSignal(signal);
                    break;
                default:
                    break;
                }
            }

            private void onNetworkBegin(
                BeginFW begin)
            {
                final long traceId = begin.traceId();

                authorization = begin.authorization();
                state = KafkaState.openedReply(state);

                doNetworkWindow(traceId, 0L, decodePool.slotCapacity(), 0);
            }

            private long networkBytesReceived;

            private void onNetworkData(
                DataFW data)
            {
                final long traceId = data.traceId();
                final long budgetId = data.budgetId();

                networkBytesReceived += Math.max(data.length(), 0);

                authorization = data.authorization();
                replyBudget -= data.reserved();

                if (replyBudget < 0)
                {
                    cleanupNetwork(traceId);
                }
                else
                {
                    if (decodeSlot == NO_SLOT)
                    {
                        decodeSlot = decodePool.acquire(initialId);
                    }

                    if (decodeSlot == NO_SLOT)
                    {
                        cleanupNetwork(traceId);
                    }
                    else
                    {
                        final OctetsFW payload = data.payload();
                        int reserved = data.reserved();
                        int offset = payload.offset();
                        int limit = payload.limit();

                        final MutableDirectBuffer buffer = decodePool.buffer(decodeSlot);
                        buffer.putBytes(decodeSlotOffset, payload.buffer(), offset, limit - offset);
                        decodeSlotOffset += limit - offset;
                        decodeSlotReserved += reserved;

                        offset = 0;
                        limit = decodeSlotOffset;
                        reserved = decodeSlotReserved;

                        decodeNetwork(traceId, authorization, budgetId, reserved, buffer, offset, limit);
                    }
                }
            }

            private void onNetworkEnd(
                EndFW end)
            {
                final long traceId = end.traceId();

                state = KafkaState.closingReply(state);

                if (decodeSlot == NO_SLOT)
                {
                    doApplicationEnd(traceId);
                }
            }

            private void onNetworkAbort(
                AbortFW abort)
            {
                final long traceId = abort.traceId();

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%s] FETCH aborted (%d bytes)\n",
                        replyId, topic, partitionId, networkBytesReceived);
                }

                state = KafkaState.closedReply(state);

                cleanupNetwork(traceId);
            }

            private void onNetworkReset(
                ResetFW reset)
            {
                final long traceId = reset.traceId();

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[client] [0x%016x] %s[%d] FETCH reset (%d bytes)\n",
                        replyId, topic, partitionId, networkBytesReceived);
                }

                state = KafkaState.closedInitial(state);

                cleanupNetwork(traceId);
            }

            private void onNetworkWindow(
                WindowFW window)
            {
                final long traceId = window.traceId();
                final long budgetId = window.budgetId();
                final int credit = window.credit();
                final int padding = window.padding();

                authorization = window.authorization();

                initialBudgetId = budgetId;
                initialBudget += credit;
                initialPadding = padding;

                state = KafkaState.openedInitial(state);

                if (encodeSlot != NO_SLOT)
                {
                    final MutableDirectBuffer buffer = encodePool.buffer(encodeSlot);
                    final int limit = encodeSlotOffset;

                    encodeNetwork(encodeSlotTraceId, authorization, budgetId, buffer, 0, limit);
                }

                doEncodeRequestIfNecessary(traceId, budgetId);
            }

            private void onNetworkSignal(
                SignalFW signal)
            {
                final long traceId = signal.traceId();
                final int signalId = signal.signalId();

                if (signalId == SIGNAL_NEXT_REQUEST)
                {
                    doEncodeRequestIfNecessary(traceId, initialBudgetId);
                }
            }

            private void doNetworkBegin(
                long traceId,
                long authorization,
                long affinity)
            {
                state = KafkaState.openingInitial(state);
                correlations.put(replyId, this::onNetwork);

                if (nextOffset == OFFSET_LIVE || nextOffset == OFFSET_HISTORICAL)
                {
                    client.encoder = client.encodeOffsetsRequest;
                    client.decoder = decodeOffsetsResponse;
                }

                Consumer<OctetsFW.Builder> extension = EMPTY_EXTENSION;

                final KafkaClientRoute clientRoute = supplyClientRoute.apply(routeId);
                final KafkaBrokerInfo broker = clientRoute.brokers.get(affinity);
                if (broker != null)
                {
                    extension = e -> e.set((b, o, l) -> proxyBeginExRW.wrap(b, o, l)
                                                                      .typeId(proxyTypeId)
                                                                      .address(a -> a.inet(i -> i.protocol(p -> p.set(STREAM))
                                                                                                 .source("0.0.0.0")
                                                                                                 .destination(broker.host)
                                                                                                 .sourcePort(0)
                                                                                                 .destinationPort(broker.port)))
                                                                      .build()
                                                                      .sizeof());
                }

                router.setThrottle(initialId, this::onNetwork);
                doBegin(network, routeId, initialId, traceId, authorization, affinity, extension);
            }

            private void doNetworkData(
                long traceId,
                long budgetId,
                DirectBuffer buffer,
                int offset,
                int limit)
            {
                if (encodeSlot != NO_SLOT)
                {
                    final MutableDirectBuffer encodeBuffer = encodePool.buffer(encodeSlot);
                    encodeBuffer.putBytes(encodeSlotOffset, buffer, offset, limit - offset);
                    encodeSlotOffset += limit - offset;
                    encodeSlotTraceId = traceId;

                    buffer = encodeBuffer;
                    offset = 0;
                    limit = encodeSlotOffset;
                }

                encodeNetwork(traceId, authorization, budgetId, buffer, offset, limit);
            }

            private void doNetworkEndAfterFlush(
                long traceId,
                long authorization)
            {
                state = KafkaState.closingInitial(state);

                if (encodeSlot == NO_SLOT)
                {
                    doNetworkEnd(traceId, authorization);
                }
            }

            private void doNetworkEnd(
                long traceId,
                long authorization)
            {
                state = KafkaState.closedInitial(state);
                doEnd(network, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);

                cleanupEncodeSlotIfNecessary();
            }

            private void doNetworkAbortIfNecessary(
                long traceId)
            {
                if (!KafkaState.initialClosed(state))
                {
                    doAbort(network, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);
                    state = KafkaState.closedInitial(state);
                }

                cleanupEncodeSlotIfNecessary();
            }

            private void doNetworkResetIfNecessary(
                long traceId)
            {
                if (!KafkaState.replyClosed(state))
                {
                    correlations.remove(replyId);
                    doReset(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
                    state = KafkaState.closedReply(state);
                }

                cleanupDecodeSlotIfNecessary();
            }

            private void doNetworkWindow(
                long traceId,
                long budgetId,
                int credit,
                int padding)
            {
                assert credit > 0 : String.format("%d > 0", credit);

                replyBudget += credit;

                doWindow(network, routeId, replyId, traceId, authorization, budgetId, credit, padding);
            }

            private void doEncodeRequestIfNecessary(
                long traceId,
                long budgetId)
            {
                if (nextRequestId == nextResponseId)
                {
                    encoder.accept(traceId, budgetId);
                }
            }

            private void doEncodeOffsetsRequest(
                long traceId,
                long budgetId)
            {
                final MutableDirectBuffer encodeBuffer = writeBuffer;
                final int encodeOffset = DataFW.FIELD_OFFSET_PAYLOAD;
                final int encodeLimit = encodeBuffer.capacity();

                int encodeProgress = encodeOffset;

                final RequestHeaderFW requestHeader = requestHeaderRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .length(0)
                        .apiKey(OFFSETS_API_KEY)
                        .apiVersion(OFFSETS_API_VERSION)
                        .correlationId(0)
                        .clientId((String) null)
                        .build();

                encodeProgress = requestHeader.limit();

                final OffsetsRequestFW offsetsRequest = offsetsRequestRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .isolationLevel(i -> i.set(READ_UNCOMMITTED))
                        .topicCount(1)
                        .build();

                encodeProgress = offsetsRequest.limit();

                final OffsetsTopicRequestFW topicRequest = offsetsTopicRequestRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .name(topic)
                        .partitionCount(1)
                        .build();

                encodeProgress = topicRequest.limit();

                final long timestamp = nextOffset;

                assert timestamp < 0;

                final OffsetsPartitionRequestFW partitionRequest = offsetsPartitionRequestRW
                        .wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .partitionId(partitionId)
                        .timestamp(timestamp)
                        .build();

                encodeProgress = partitionRequest.limit();

                final int requestId = nextRequestId++;
                final int requestSize = encodeProgress - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY;

                requestHeaderRW.wrap(encodeBuffer, requestHeader.offset(), requestHeader.limit())
                        .length(requestSize)
                        .apiKey(requestHeader.apiKey())
                        .apiVersion(requestHeader.apiVersion())
                        .correlationId(requestId)
                        .clientId(requestHeader.clientId().asString())
                        .build();

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[0x%016x] %s[%d] OFFSETS %d\n", replyId, topic, partitionId, timestamp);
                }

                doNetworkData(traceId, budgetId, encodeBuffer, encodeOffset, encodeProgress);
            }

            private void doEncodeFetchRequest(
                long traceId,
                long budgetId)
            {
                final MutableDirectBuffer encodeBuffer = writeBuffer;
                final int encodeOffset = DataFW.FIELD_OFFSET_PAYLOAD;
                final int encodeLimit = encodeBuffer.capacity();

                int encodeProgress = encodeOffset;

                final RequestHeaderFW requestHeader = requestHeaderRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .length(0)
                        .apiKey(FETCH_API_KEY)
                        .apiVersion(FETCH_API_VERSION)
                        .correlationId(0)
                        .clientId((String) null)
                        .build();

                encodeProgress = requestHeader.limit();

                final FetchRequestFW fetchRequest = fetchRequestRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .maxWaitTimeMillis(!KafkaState.replyOpened(stream.state) ? 0 : fetchMaxWaitMillis)
                        .minBytes(1)
                        .maxBytes(fetchMaxBytes)
                        .isolationLevel((byte) 0)
                        .topicCount(1)
                        .build();

                encodeProgress = fetchRequest.limit();

                final TopicRequestFW topicRequest = fetchTopicRequestRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .name(topic)
                        .partitionCount(1)
                        .build();

                encodeProgress = topicRequest.limit();

                final PartitionRequestFW partitionRequest = fetchPartitionRequestRW
                        .wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .partitionId((int) partitionId)
                        .fetchOffset(nextOffset)
                        .maxBytes(partitionMaxBytes)
                        .build();

                encodeProgress = partitionRequest.limit();

                final int requestId = nextRequestId++;
                final int requestSize = encodeProgress - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY;

                requestHeaderRW.wrap(encodeBuffer, requestHeader.offset(), requestHeader.limit())
                        .length(requestSize)
                        .apiKey(requestHeader.apiKey())
                        .apiVersion(requestHeader.apiVersion())
                        .correlationId(requestId)
                        .clientId(requestHeader.clientId().asString())
                        .build();

                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[0x%016x] %s[%d] FETCH %d\n", replyId, topic, partitionId, nextOffset);
                }

                doNetworkData(traceId, budgetId, encodeBuffer, encodeOffset, encodeProgress);
            }

            private void encodeNetwork(
                long traceId,
                long authorization,
                long budgetId,
                DirectBuffer buffer,
                int offset,
                int limit)
            {
                final int maxLength = limit - offset;
                final int length = Math.max(Math.min(initialBudget - initialPadding, maxLength), 0);

                if (length > 0)
                {
                    final int reserved = length + initialPadding;

                    initialBudget -= reserved;

                    assert initialBudget >= 0 : String.format("%d >= 0", initialBudget);

                    doData(network, routeId, initialId, traceId, authorization, budgetId,
                           reserved, buffer, offset, length, EMPTY_OCTETS);
                }

                final int remaining = maxLength - length;
                if (remaining > 0)
                {
                    if (encodeSlot == NO_SLOT)
                    {
                        encodeSlot = encodePool.acquire(initialId);
                    }

                    if (encodeSlot == NO_SLOT)
                    {
                        cleanupNetwork(traceId);
                    }
                    else
                    {
                        final MutableDirectBuffer encodeBuffer = encodePool.buffer(encodeSlot);
                        encodeBuffer.putBytes(0, buffer, offset + length, remaining);
                        encodeSlotOffset = remaining;
                    }
                }
                else
                {
                    cleanupEncodeSlotIfNecessary();

                    if (KafkaState.initialClosing(state))
                    {
                        doNetworkEnd(traceId, authorization);
                    }
                }
            }

            private void decodeNetworkIfNecessary(
                long traceId)
            {
                if (decodeSlot != NO_SLOT)
                {
                    final MutableDirectBuffer buffer = decodePool.buffer(decodeSlot);
                    final long budgetId = 0L; // TODO
                    final int offset = 0;
                    final int limit = decodeSlotOffset;
                    final int reserved = decodeSlotReserved;

                    decodeNetwork(traceId, authorization, budgetId, reserved, buffer, offset, limit);
                }
            }

            private void decodeNetwork(
                long traceId,
                long authorization,
                long budgetId,
                int reserved,
                MutableDirectBuffer buffer,
                int offset,
                int limit)
            {
                KafkaFetchClientDecoder previous = null;
                int progress = offset;
                while (progress <= limit && previous != decoder)
                {
                    previous = decoder;
                    progress = decoder.decode(this, traceId, authorization, budgetId, reserved, buffer, offset, progress, limit);
                }

                if (progress < limit)
                {
                    if (decodeSlot == NO_SLOT)
                    {
                        decodeSlot = decodePool.acquire(initialId);
                    }

                    if (decodeSlot == NO_SLOT)
                    {
                        cleanupNetwork(traceId);
                    }
                    else
                    {
                        final MutableDirectBuffer decodeBuffer = decodePool.buffer(decodeSlot);
                        decodeBuffer.putBytes(0, buffer, progress, limit - progress);
                        decodeSlotOffset = limit - progress;
                        decodeSlotReserved = (int) ((long) (limit - progress) * reserved / (limit - offset));
                        assert decodeSlotReserved >= 0;
                    }

                    final int credit = decodePool.slotCapacity() - decodeSlotOffset - replyBudget;
                    if (credit > 0)
                    {
                        doNetworkWindow(traceId, budgetId, credit, 0);
                    }
                }
                else
                {
                    cleanupDecodeSlotIfNecessary();

                    if (KafkaState.replyClosing(state))
                    {
                        doApplicationEnd(traceId);
                    }
                    else if (reserved > 0)
                    {
                        doNetworkWindow(traceId, budgetId, reserved, 0);
                    }
                }
            }

            private void onDecodeOffsetsPartition(
                long traceId,
                long authorization,
                int errorCode,
                int partitionId,
                long partitionOffset)
            {
                switch (errorCode)
                {
                case ERROR_NONE:
                    assert partitionId == this.partitionId;
                    this.nextOffset = partitionOffset;
                    break;
                default:
                    cleanupApplication(traceId, errorCode);
                    doNetworkEnd(traceId, authorization);
                    break;
                }
            }

            private void onDecodeFetchPartition(
                long traceId,
                long authorization,
                int partitionId,
                int errorCode)
            {
                switch (errorCode)
                {
                case ERROR_NONE:
                    assert partitionId == this.partitionId;
                    doApplicationWindow(traceId, 0L, 0, 0);
                    doApplicationBeginIfNecessary(traceId, authorization, topic, partitionId, nextOffset, latestOffset);
                    break;
                case ERROR_OFFSET_OUT_OF_RANGE:
                    assert partitionId == this.partitionId;
                    // TODO: recover at EARLIEST or LATEST ?
                    nextOffset = OFFSET_HISTORICAL;
                    client.encoder = client.encodeOffsetsRequest;
                    client.decoder = decodeOffsetsResponse;
                    doEncodeRequestIfNecessary(traceId, initialBudgetId);
                    break;
                default:
                    if (errorCode == ERROR_NOT_LEADER_FOR_PARTITION)
                    {
                        final long metaInitialId = clientRoute.metaInitialId;
                        if (metaInitialId != 0L)
                        {
                            final MessageConsumer metaInitial = router.supplyReceiver(metaInitialId);
                            doFlush(metaInitial, routeId, metaInitialId, traceId, authorization, EMPTY_EXTENSION);
                        }
                    }

                    cleanupApplication(traceId, errorCode);
                    doNetworkEnd(traceId, authorization);
                    break;
                }
            }

            private void onDecodeFetchRecord(
                long traceId,
                int reserved,
                long offset,
                long timestamp,
                OctetsFW key,
                OctetsFW value,
                int headerCount,
                DirectBuffer headers)
            {
                this.nextOffset = offset + 1;

                final KafkaDataExFW kafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                        .typeId(kafkaTypeId)
                        .fetch(f ->
                        {
                            f.timestamp(timestamp);
                            f.partition(p -> p.partitionId(decodePartitionId).partitionOffset(offset).latestOffset(latestOffset));
                            f.key(k -> setKey(k, key));
                            final int headersLimit = headers.capacity();
                            int headerProgress = 0;
                            for (int headerIndex = 0; headerIndex < headerCount; headerIndex++)
                            {
                                final MessageHeaderFW header = messageHeaderRO.wrap(headers, headerProgress, headersLimit);
                                f.headersItem(i -> setHeader(i, header.key(), header.value()));
                                headerProgress = header.limit();
                            }
                        })
                        .build();

                doApplicationData(traceId, authorization, FLAG_INIT | FLAG_FIN, reserved, value, kafkaDataEx);
            }

            private void onDecodeFetchRecordValueInit(
                long traceId,
                int reserved,
                int deferred,
                long offset,
                long timestamp,
                int headersSizeMax,
                OctetsFW key,
                OctetsFW valueInit)
            {
                final KafkaDataExFW kafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.deferred(deferred)
                                     .timestamp(timestamp)
                                     .headersSizeMax(headersSizeMax)
                                     .partition(p -> p.partitionId(decodePartitionId)
                                                      .partitionOffset(offset)
                                                      .latestOffset(latestOffset))
                                     .key(k -> setKey(k, key)))
                        .build();

                doApplicationData(traceId, authorization, FLAG_INIT, reserved, valueInit, kafkaDataEx);
            }

            private void onDecodeFetchRecordValueCont(
                long traceId,
                int reserved,
                OctetsFW value)
            {
                doApplicationData(traceId, authorization, FLAG_CONT, reserved, value, EMPTY_OCTETS);
            }

            private void onDecodeFetchRecordValueFin(
                long traceId,
                int reserved,
                long offset,
                OctetsFW value,
                int headerCount,
                DirectBuffer headers)
            {
                this.nextOffset = offset + 1;

                final KafkaDataExFW kafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                        .typeId(kafkaTypeId)
                        .fetch(f ->
                        {
                            f.partition(p -> p.partitionId(decodePartitionId).partitionOffset(offset).latestOffset(latestOffset));
                            final int headersLimit = headers.capacity();
                            int headerProgress = 0;
                            for (int headerIndex = 0; headerIndex < headerCount; headerIndex++)
                            {
                                final MessageHeaderFW header = messageHeaderRO.wrap(headers, headerProgress, headersLimit);
                                f.headersItem(i -> setHeader(i, header.key(), header.value()));
                                headerProgress = header.limit();
                            }
                        })
                        .build();

                doApplicationData(traceId, authorization, FLAG_FIN, reserved, value, kafkaDataEx);
            }

            private void onDecodeResponse(
                long traceId)
            {
                nextResponseId++;

                if (clientRoute.partitions.get(partitionId) == leaderId)
                {
                    signaler.signalNow(routeId, initialId, SIGNAL_NEXT_REQUEST);
                }
                else
                {
                    cleanupApplication(traceId, ERROR_NOT_LEADER_FOR_PARTITION);
                    doNetworkEnd(traceId, authorization);
                }
            }

            private void cleanupNetwork(
                long traceId)
            {
                doNetworkResetIfNecessary(traceId);
                doNetworkAbortIfNecessary(traceId);

                cleanupApplication(traceId, EMPTY_OCTETS);
            }

            private void cleanupDecodeSlotIfNecessary()
            {
                if (decodeSlot != NO_SLOT)
                {
                    decodePool.release(decodeSlot);
                    decodeSlot = NO_SLOT;
                    decodeSlotOffset = 0;
                    decodeSlotReserved = 0;
                }
            }

            private void cleanupEncodeSlotIfNecessary()
            {
                if (encodeSlot != NO_SLOT)
                {
                    encodePool.release(encodeSlot);
                    encodeSlot = NO_SLOT;
                    encodeSlotOffset = 0;
                    encodeSlotTraceId = 0;
                }
            }
        }
    }

    private DirectBuffer wrapHeaders(
        DirectBuffer buffer,
        int offset,
        int length)
    {
        DirectBuffer headers = EMPTY_BUFFER;
        if (length != 0)
        {
            headers = headersRO;
            headers.wrap(buffer, offset, length);
        }
        return headers;
    }

    private static KafkaKeyFW.Builder setKey(
        KafkaKeyFW.Builder key,
        OctetsFW value)
    {
        if (value == null)
        {
            key.length(-1).value((OctetsFW) null);
        }
        else
        {
            final int sizeOfValue = value.sizeof();
            key.length(sizeOfValue).value(value.buffer(), value.offset(), sizeOfValue);
        }

        return key;
    }

    private static KafkaHeaderFW.Builder setHeader(
        KafkaHeaderFW.Builder header,
        OctetsFW name,
        OctetsFW value)
    {
        if (name == null)
        {
            header.nameLen(-1).name((OctetsFW) null);
        }
        else
        {
            final int sizeOfName = name.sizeof();
            header.nameLen(sizeOfName).name(name.buffer(), name.offset(), sizeOfName);
        }

        if (value == null)
        {
            header.valueLen(-1).value((OctetsFW) null);
        }
        else
        {
            final int sizeOfValue = value.sizeof();
            header.valueLen(sizeOfValue).value(value.buffer(), value.offset(), sizeOfValue);
        }

        return header;
    }

    private static boolean isCompressedBatch(
        int attributes)
    {
        // 0 = NONE, 1 = GZIP, 2 = SNAPPY, 3 = LZ4
        return (attributes & 0x07) != 0;
    }

    private static boolean isControlBatch(
        int attributes)
    {
        // sixth lowest bit indicates whether the RecordBatch includes a control message
        return (attributes & 0x20) != 0;
    }
}
