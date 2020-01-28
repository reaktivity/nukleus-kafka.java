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
package org.reaktivity.nukleus.kafka.internal.stream;

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.kafka.internal.types.codec.offsets.IsolationLevel.READ_UNCOMMITTED;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongLongConsumer;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;
import org.reaktivity.nukleus.kafka.internal.types.Varint32FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.RequestHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.ResponseHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.MessageHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordBatchFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordTrailerFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TransactionResponseFW;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFetchBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaResetExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaClientFetchFactory implements StreamFactory
{
    private static final int FIELD_LIMIT_RECORD_BATCH_LENGTH = RecordBatchFW.FIELD_OFFSET_LEADER_EPOCH;

    private static final int FLAG_CONT = 0x00;
    private static final int FLAG_FIN = 0x01;
    private static final int FLAG_INIT = 0x02;

    private static final long OFFSET_LATEST = -1L;
    private static final long OFFSET_EARLIEST = -2L;

    private static final int SIGNAL_NEXT_REQUEST = 1;

    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(new byte[0]), 0, 0);
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};
    private static final byte[] ANY_IP_ADDR = new byte[4];

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
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();
    private final KafkaDataExFW.Builder kafkaDataExRW = new KafkaDataExFW.Builder();
    private final TcpBeginExFW.Builder tcpBeginExRW = new TcpBeginExFW.Builder();

    private final RequestHeaderFW.Builder requestHeaderRW = new RequestHeaderFW.Builder();
    private final OffsetsRequestFW.Builder offsetsRequestRW = new OffsetsRequestFW.Builder();
    private final OffsetsTopicRequestFW.Builder offsetsTopicRequestRW = new OffsetsTopicRequestFW.Builder();
    private final OffsetsPartitionRequestFW.Builder offsetsPartitionRequestRW = new OffsetsPartitionRequestFW.Builder();
    private final Long2LongHashMap offsetsUnresolved = new Long2LongHashMap(Long.MIN_VALUE);
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
    private final KafkaFetchClientDecoder decodeOffsetsTopic = this::decodeOffsetsTopic;
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
    private final KafkaFetchClientDecoder decodeIgnoreRecordSet = this::decodeIgnoreRecordSet;
    private final KafkaFetchClientDecoder decodeIgnoreAll = this::decodeIgnoreAll;

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final int fetchMaxBytes;
    private final int partitionMaxBytes;
    private final int kafkaTypeId;
    private final int tcpTypeId;
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
    private final Long2ObjectHashMap<Long2ObjectHashMap<KafkaBrokerInfo>> brokersByRouteId;
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
        Long2ObjectHashMap<Long2ObjectHashMap<KafkaBrokerInfo>> brokersByRouteId)
    {
        this.fetchMaxBytes = config.fetchMaxBytes();
        this.partitionMaxBytes = config.fetchPartitionMaxBytes();
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.tcpTypeId = supplyTypeId.applyAsInt("tcp");
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
        this.brokersByRouteId = brokersByRouteId;
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
        final long affinity = begin.affinity();
        final long authorization = begin.authorization();
        final OctetsFW extension = begin.extension();
        final ExtensionFW beginEx = extensionRO.tryWrap(extension.buffer(), extension.offset(), extension.limit());
        final KafkaBeginExFW kafkaBeginEx = beginEx.typeId() == kafkaTypeId ? extension.get(kafkaBeginExRO::wrap) : null;
        assert kafkaBeginEx == null || kafkaBeginEx.kind() == KafkaBeginExFW.KIND_FETCH;
        final KafkaFetchBeginExFW kafkaFetchBeginEx = kafkaBeginEx != null ? kafkaBeginEx.fetch() : null;

        MessageConsumer newStream = null;

        if (beginEx != null && kafkaFetchBeginEx != null &&
            !kafkaFetchBeginEx.progress().isEmpty() &&
            kafkaFetchBeginEx.filters().isEmpty())
        {

            final String16FW beginTopic = kafkaFetchBeginEx.topic();

            final MessagePredicate filter = (t, b, i, l) ->
            {
                final RouteFW route = wrapRoute.apply(t, b, i, l);
                final OctetsFW routeEx = route.extension();
                final KafkaRouteExFW kafkaRouteEx = routeEx.get(kafkaRouteExRO::tryWrap);
                final String16FW routeTopic = kafkaRouteEx.topic();
                return !route.localAddress().equals(route.remoteAddress()) &&
                        Objects.equals(routeTopic, beginTopic);
            };

            final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);

            if (route != null)
            {
                final long resolvedId = route.correlationId();
                final String topic = beginTopic != null ? beginTopic.asString() : null;
                final Long2LongHashMap progress = new Long2LongHashMap(Long.MIN_VALUE);
                final MutableBoolean repeats = new MutableBoolean();
                final long missingValue = progress.missingValue();
                kafkaFetchBeginEx.progress()
                                 .forEach(o -> repeats.value |= progress.put(o.partitionId(), o.offset$()) != missingValue);

                if (!repeats.value)
                {
                    newStream = new KafkaFetchStream(
                            application,
                            routeId,
                            initialId,
                            affinity,
                            resolvedId,
                            topic,
                            progress)::onApplication;
                }
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
        long authorization)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .traceId(traceId)
               .authorization(authorization)
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
                client.decoder = decodeOffsetsTopic;
            }
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
        if (client.decodableTopics == 0)
        {
            client.onDecodeResponse(traceId);
            client.encoder = client.encodeFetchRequest;
            client.decoder = decodeFetchResponse;
            break decode;
        }
        else if (length != 0)
        {
            final OffsetsTopicResponseFW topic = offsetsTopicResponseRO.tryWrap(buffer, progress, limit);
            if (topic != null)
            {
                final String topicName = topic.name().asString();
                assert client.topic.equals(topicName);

                progress = topic.limit();

                client.decodableResponseBytes -= topic.sizeof();
                assert client.decodableResponseBytes >= 0;

                client.decodablePartitions = topic.partitionCount();
                client.decoder = decodeOffsetsPartition;
            }
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
            if (partition != null)
            {
                final int partitionId = partition.partitionId();
                final int errorCode = partition.errorCode();
                final long partitionOffset = partition.offset$();

                progress = partition.limit();

                client.decodableResponseBytes -= partition.sizeof();
                assert client.decodableResponseBytes >= 0;

                client.decodablePartitions--;
                assert client.decodablePartitions >= 0;

                client.onDecodeOffsetsPartition(traceId, authorization, errorCode, partitionId, partitionOffset);

                if (client.decodablePartitions == 0)
                {
                    client.decodableTopics--;
                    assert client.decodableTopics >= 0;
                    client.decoder = decodeOffsetsTopic;
                    break decode;
                }

                client.decoder = decodeOffsetsPartition;
            }
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
                final int responseProgress = recordSet.sizeof();

                progress += responseProgress;

                client.decodableResponseBytes -= responseProgress;
                assert client.decodableResponseBytes >= 0;

                client.decodableRecordSetBytes = recordSet.length();
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

            if (recordBatch != null)
            {
                final int recordSetProgress = recordBatch.sizeof();
                final int recordBatchProgress = recordSetProgress - FIELD_LIMIT_RECORD_BATCH_LENGTH;

                progress += recordSetProgress;

                client.decodableRecordBatchBytes = recordBatch.length();
                client.decodeRecordBatchOffset = recordBatch.firstOffset();
                client.decodeRecordBatchTimestamp = recordBatch.firstTimestamp();
                client.decodableRecords = recordBatch.recordCount();

                client.decodableResponseBytes -= recordSetProgress;
                assert client.decodableResponseBytes >= 0;

                client.decodableRecordSetBytes -= recordSetProgress;
                assert client.decodableRecordSetBytes >= 0;

                client.decodableRecordBatchBytes -= recordBatchProgress;
                assert client.decodableRecordBatchBytes >= 0;

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
            client.decoder = decodeFetchRecordBatch;
            break decode;
        }
        else if (length != 0)
        {
            final Varint32FW recordLength = recordLengthRO.tryWrap(buffer, progress, limit);

            // TODO: verify incremental decode vs truncated record set mid record length
            if (recordLength == null && length >= client.decodableRecordSetBytes)
            {
                client.decoder = decodeIgnoreRecordSet;
                break decode;
            }

            if (recordLength != null)
            {
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
                final long timestamp = client.decodeRecordBatchTimestamp + recordHeader.timestampDelta();
                final OctetsFW key = recordHeader.key();

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
                        valueClaimed = client.stream.replyDebitor.claim(client.stream.replyDebitorIndex,
                                client.stream.replyId, minimum, maximum);

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
                        final DirectBuffer headers = headersRO;
                        headers.wrap(buffer, headersOffset, headersLength);

                        progress += sizeofRecord;

                        client.onDecodeFetchRecord(traceId, valueReserved, offsetAbs, timestamp,
                                key, value, headerCount, headers);

                        client.decodableRecords--;
                        assert client.decodableRecords >= 0;

                        client.decodableResponseBytes -= sizeofRecord;
                        assert client.decodableResponseBytes >= 0;

                        client.decodableRecordBatchBytes -= sizeofRecord;
                        assert client.decodableRecordBatchBytes >= 0;

                        client.decodableRecordSetBytes -= sizeofRecord;
                        assert client.decodableRecordSetBytes >= 0;

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

        if (length >= decodeMaxBytes)
        {
            final RecordHeaderFW recordHeader = recordHeaderRO.tryWrap(buffer, progress, limit);
            if (recordHeader != null)
            {
                final Varint32FW recordLength = recordLengthRO.tryWrap(buffer, progress, limit);
                assert recordHeader.length() == recordLength.value();

                final int sizeofRecord = recordLength.sizeof() + recordLength.value();
                final long offsetAbs = client.decodeRecordBatchOffset + recordHeader.offsetDelta();
                final long timestamp = client.decodeRecordBatchTimestamp + recordHeader.timestampDelta();
                final OctetsFW key = recordHeader.key();

                final int valueLength = recordHeader.valueLength();
                final int valueOffset = recordHeader.limit();

                final int valueSize = Math.max(valueLength, 0);
                final int valueReserved = valueSize + client.stream.replyPadding;
                final int valueReservedMax = Math.min(valueReserved, client.stream.replyBudget);
                final int valueLengthMax = valueReservedMax - client.stream.replyPadding;
                final int valueLimitMax = Math.min(limit, valueOffset + valueLengthMax);
                final OctetsFW valueInit = valueLength != -1 ? valueRO.wrap(buffer, valueOffset, valueLimitMax) : null;
                final int valueProgress = valueInit != null ? valueInit.sizeof() : 0;
                final int recordProgress = recordHeader.sizeof() + valueProgress;

                progress += recordProgress;

                client.onDecodeFetchRecordValueInit(traceId, valueReservedMax, offsetAbs, timestamp, key, valueInit);

                client.decodeRecordOffset = offsetAbs;
                client.decodableRecordValueBytes = valueLength != -1 ? valueLength - valueProgress : -1;
                client.decodableRecordBytes = sizeofRecord;

                client.decodableResponseBytes -= recordProgress;
                assert client.decodableResponseBytes >= 0;

                client.decodableRecordBatchBytes -= recordProgress;
                assert client.decodableRecordBatchBytes >= 0;

                client.decodableRecordSetBytes -= recordProgress;
                assert client.decodableRecordSetBytes >= 0;

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
        final int valueLengthMax = Math.max(length, client.stream.replyBudget - client.stream.replyPadding);

        if (length >= client.decodableRecordBytes && valueLengthMax >= client.decodableRecordValueBytes)
        {
            final int valueOffset = progress;
            final int valueProgress = client.decodableRecordValueBytes;
            final int valueSize = Math.max(valueProgress, 0);
            final int valueLimit = valueOffset + valueSize;
            final OctetsFW valueFin = valueProgress != -1 ? valueRO.wrap(buffer, valueOffset, valueLimit) : null;
            final int valueReserved = valueSize + client.stream.replyPadding;

            final int recordProgress = client.decodableRecordBytes;
            final int recordLimit = progress + recordProgress;
            final RecordTrailerFW recordTrailer = recordTrailerRO.wrap(buffer, valueLimit, recordLimit);
            final int headerCount = recordTrailer.headerCount();
            final int headersOffset = recordTrailer.limit();
            final int headersLength = recordLimit - headersOffset;
            final DirectBuffer headers = headersRO;
            headers.wrap(buffer, headersOffset, headersLength);

            progress += recordProgress;

            client.onDecodeFetchRecordValueFin(traceId, valueReserved, client.decodeRecordOffset, valueFin, headerCount, headers);

            client.decodableRecords--;
            assert client.decodableRecords >= 0;

            client.decodableResponseBytes -= recordProgress;
            assert client.decodableResponseBytes >= 0;

            client.decodableRecordBatchBytes -= recordProgress;
            assert client.decodableRecordBatchBytes >= 0;

            client.decodableRecordSetBytes -= recordProgress;
            assert client.decodableRecordSetBytes >= 0;

            client.decodableRecordBytes -= recordProgress;
            assert client.decodableRecordBytes == 0;

            client.decodableRecordValueBytes -= valueProgress;
            assert client.decodableRecordValueBytes == 0;

            client.decoder = decodeFetchRecordLength;
        }
        else if (length >= decodeMaxBytes)
        {
            assert client.decodableRecordValueBytes != -1 : "record headers overflow decode buffer";

            final int valueOffset = progress;
            final int valueProgress = Math.min(client.decodableRecordValueBytes, decodeMaxBytes);
            final int valueLimit = valueOffset + valueProgress;
            final int valueReserved = valueProgress + client.stream.replyPadding;
            final OctetsFW value = valueRO.wrap(buffer, valueOffset, valueLimit);

            progress += valueProgress;

            client.onDecodeFetchRecordValueCont(traceId, valueReserved, value);

            client.decodableRecordBytes -= valueProgress;
            assert client.decodableRecordBytes >= 0;

            client.decodableRecordValueBytes -= valueProgress;
            assert client.decodableRecordValueBytes >= 0;
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
        client.decodableRecordSetBytes -= length;

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
        private final long affinity;
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
            long affinity,
            long resolvedId,
            String topic,
            Long2LongHashMap progress)
        {
            this.application = application;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.affinity = affinity;
            this.client = new KafkaFetchClient(resolvedId, topic, progress);
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

            client.doNetworkBegin(traceId, authorization, affinity);
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
            Long2LongHashMap progress)
        {
            if (!KafkaState.replyOpening(state))
            {
                doApplicationBegin(traceId, authorization, topic, progress);
            }
        }

        private void doApplicationBegin(
            long traceId,
            long authorization,
            String topic,
            Long2LongHashMap progress)
        {
            state = KafkaState.openingReply(state);

            router.setThrottle(replyId, this::onApplication);
            doBegin(application, routeId, replyId, traceId, authorization, affinity,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                                                        .typeId(kafkaTypeId)
                                                        .fetch(m -> m.topic(topic)
                                                                     .progress(ps -> visitProgress(ps, progress)))
                                                        .build()
                                                        .sizeof()));
        }

        private void visitProgress(
            ArrayFW.Builder<KafkaOffsetFW.Builder, KafkaOffsetFW> builder,
            Long2LongHashMap progress)
        {
            progress.longForEach((id, p) -> builder.item(i -> i.partitionId((int) id).offset$(p)));
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

            doReset(application, routeId, initialId, traceId, client.authorization);
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
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doApplicationReset(traceId, extension);
            }
        }

        private void cleanupApplication(
            long traceId,
            Flyweight extension)
        {
            doApplicationAbortIfNecessary(traceId);
            doApplicationResetIfNecessary(traceId, extension);
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
            private final Long2LongHashMap progress;

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
            private long decodeRecordBatchTimestamp;
            private int decodableRecords;
            private long decodeRecordOffset;
            private int decodableRecordBytes;
            private int decodableRecordValueBytes;

            private int nextRequestId;
            private int nextResponseId;

            private KafkaFetchClientDecoder decoder;
            private LongLongConsumer encoder;
            private final KafkaResetExFW.Builder resetExRW = new KafkaResetExFW.Builder();

            KafkaFetchClient(
                long routeId,
                String topic,
                Long2LongHashMap progress)
            {
                this.stream = KafkaFetchStream.this;
                this.routeId = routeId;
                this.initialId = supplyInitialId.applyAsLong(routeId);
                this.replyId = supplyReplyId.applyAsLong(initialId);
                this.network = router.supplyReceiver(initialId);
                this.topic = requireNonNull(topic);
                this.progress = progress;
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

            private void onNetworkData(
                DataFW data)
            {
                final long traceId = data.traceId();
                final long budgetId = data.budgetId();

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

                state = KafkaState.closedReply(state);

                if (decodeSlot == NO_SLOT)
                {
                    doApplicationEnd(traceId);
                }
            }

            private void onNetworkAbort(
                AbortFW abort)
            {
                final long traceId = abort.traceId();

                state = KafkaState.closedReply(state);

                cleanupNetwork(traceId);
            }

            private void onNetworkReset(
                ResetFW reset)
            {
                final long traceId = reset.traceId();

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

                if (progress.containsValue(OFFSET_LATEST) || progress.containsValue(OFFSET_EARLIEST))
                {
                    client.encoder = client.encodeOffsetsRequest;
                    client.decoder = decodeOffsetsResponse;
                }

                Consumer<OctetsFW.Builder> extension = EMPTY_EXTENSION;

                final Long2ObjectHashMap<KafkaBrokerInfo> brokers = brokersByRouteId.get(routeId);
                final KafkaBrokerInfo broker = brokers != null ? brokers.get(affinity) : null;
                if (broker != null)
                {
                    extension = e -> e.set((b, o, l) -> tcpBeginExRW.wrap(b, o, l)
                                                                    .typeId(tcpTypeId)
                                                                    .localAddress(a -> a.ipv4Address(ip -> ip.put(ANY_IP_ADDR)))
                                                                    .localPort(0)
                                                                    .remoteAddress(a -> a.host(broker.host))
                                                                    .remotePort(broker.port)
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
                    doReset(network, routeId, replyId, traceId, authorization);
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

                offsetsUnresolved.clear();
                for (Long2LongHashMap.EntryIterator i = progress.entrySet().iterator(); i.hasNext(); )
                {
                    i.next();
                    final long partitionId = i.getLongKey();
                    final long partitionOffset = i.getLongValue();

                    if (partitionOffset == OFFSET_LATEST || partitionOffset == OFFSET_EARLIEST)
                    {
                        offsetsUnresolved.put(partitionId, partitionOffset);
                    }
                }

                final OffsetsTopicRequestFW topicRequest = offsetsTopicRequestRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .name(topic)
                        .partitionCount(offsetsUnresolved.size())
                        .build();

                encodeProgress = topicRequest.limit();

                for (Long2LongHashMap.EntryIterator i = offsetsUnresolved.entrySet().iterator(); i.hasNext(); )
                {
                    i.next();
                    final long partitionId = i.getLongKey();
                    final long timestamp = i.getLongValue();

                    assert timestamp < 0;

                    final OffsetsPartitionRequestFW partitionRequest = offsetsPartitionRequestRW
                            .wrap(encodeBuffer, encodeProgress, encodeLimit)
                            .partitionId((int) partitionId)
                            .timestamp(timestamp)
                            .build();

                    encodeProgress = partitionRequest.limit();
                }

                final int requestId = nextRequestId++;
                final int requestSize = encodeProgress - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY;

                requestHeaderRW.wrap(encodeBuffer, requestHeader.offset(), requestHeader.limit())
                        .length(requestSize)
                        .apiKey(requestHeader.apiKey())
                        .apiVersion(requestHeader.apiVersion())
                        .correlationId(requestId)
                        .clientId(requestHeader.clientId().asString())
                        .build();

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
                        .maxWaitTimeMillis(500)
                        .minBytes(1)
                        .maxBytes(fetchMaxBytes)
                        .isolationLevel((byte) 0)
                        .topicCount(1)
                        .build();

                encodeProgress = fetchRequest.limit();

                final TopicRequestFW topicRequest = fetchTopicRequestRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .name(topic)
                        .partitionCount(progress.size())
                        .build();

                encodeProgress = topicRequest.limit();

                for (Long2LongHashMap.EntryIterator i = progress.entrySet().iterator(); i.hasNext(); )
                {
                    i.next();
                    final long partitionId = i.getLongKey();
                    final long offset = i.getLongValue();

                    final PartitionRequestFW partitionRequest = fetchPartitionRequestRW
                            .wrap(encodeBuffer, encodeProgress, encodeLimit)
                            .partitionId((int) partitionId)
                            .fetchOffset(offset)
                            .maxBytes(partitionMaxBytes)
                            .build();

                    encodeProgress = partitionRequest.limit();
                }

                final int requestId = nextRequestId++;
                final int requestSize = encodeProgress - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY;

                requestHeaderRW.wrap(encodeBuffer, requestHeader.offset(), requestHeader.limit())
                        .length(requestSize)
                        .apiKey(requestHeader.apiKey())
                        .apiVersion(requestHeader.apiVersion())
                        .correlationId(requestId)
                        .clientId(requestHeader.clientId().asString())
                        .build();

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
                        decodeSlotReserved = (limit - progress) * reserved / (limit - offset);
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
                    else
                    {
                        final int credit = progress - offset;
                        if (credit > 0)
                        {
                            doNetworkWindow(traceId, budgetId, credit, 0);
                        }
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
                if (errorCode == 0)
                {
                    progress.put(partitionId, partitionOffset);
                }
                else
                {
                    final KafkaResetExFW resetEx = resetExRW.wrap(extBuffer, 0, extBuffer.capacity())
                                                            .typeId(kafkaTypeId)
                                                            .error(errorCode)
                                                            .build();
                    cleanupApplication(traceId, resetEx);
                    doNetworkEnd(traceId, authorization);
                }
            }

            private void onDecodeFetchPartition(
                long traceId,
                long authorization,
                int partitionId,
                int errorCode)
            {
                if (errorCode == 0)
                {
                    doApplicationWindow(traceId, 0L, 0, 0);
                    doApplicationBeginIfNecessary(traceId, authorization, topic, progress);
                }
                else
                {
                    final KafkaResetExFW resetEx = resetExRW.wrap(extBuffer, 0, extBuffer.capacity())
                                                            .typeId(kafkaTypeId)
                                                            .error(errorCode)
                                                            .build();
                    cleanupApplication(traceId, resetEx);
                    doNetworkEnd(traceId, authorization);
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
                progress.put(decodePartitionId, offset + 1);

                final KafkaDataExFW kafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                        .typeId(kafkaTypeId)
                        .fetch(f ->
                        {
                            f.timestamp(timestamp);
                            f.key(k -> k.value(key));
                            final int headersLimit = headers.capacity();
                            int headerProgress = 0;
                            for (int headerIndex = 0; headerIndex < headerCount; headerIndex++)
                            {
                                final MessageHeaderFW header = messageHeaderRO.wrap(headers, headerProgress, headersLimit);
                                f.headersItem(i -> setName(i, header.key()).value(header.value()));
                                headerProgress = header.limit();
                            }
                            f.progressItem(p -> progress.longForEach((id, next) -> p.partitionId((int) id).offset$(next)));
                        })
                        .build();

                doApplicationData(traceId, authorization, FLAG_INIT | FLAG_FIN, reserved, value, kafkaDataEx);
            }

            private void onDecodeFetchRecordValueInit(
                long traceId,
                int reserved,
                long offset,
                long timestamp,
                OctetsFW key,
                OctetsFW valueInit)
            {
                final KafkaDataExFW kafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.timestamp(timestamp).key(k -> k.value(key)))
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
                progress.put(decodePartitionId, offset + 1);

                final KafkaDataExFW kafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                        .typeId(kafkaTypeId)
                        .fetch(f ->
                        {
                            final int headersLimit = headers.capacity();
                            int headerProgress = 0;
                            for (int headerIndex = 0; headerIndex < headerCount; headerIndex++)
                            {
                                final MessageHeaderFW header = messageHeaderRO.wrap(headers, headerProgress, headersLimit);
                                f.headersItem(i -> setName(i, header.key()).value(header.value()));
                                headerProgress = header.limit();
                            }
                            f.progressItem(p -> progress.longForEach((id, next) -> p.partitionId((int) id).offset$(next)));
                        })
                        .build();

                doApplicationData(traceId, authorization, FLAG_FIN, reserved, value, kafkaDataEx);
            }

            private void onDecodeResponse(
                long traceId)
            {
                nextResponseId++;
                signaler.signalNow(routeId, initialId, SIGNAL_NEXT_REQUEST);
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

    private static KafkaHeaderFW.Builder setName(
        KafkaHeaderFW.Builder header,
        OctetsFW name)
    {
        if (name == null)
        {
            header.name((String16FW) null);
        }
        else
        {
            header.name(name.buffer(), name.offset(), name.sizeof());
        }

        return header;
    }
}
