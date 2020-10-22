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

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.kafka.internal.types.codec.RequestHeaderFW.FIELD_OFFSET_API_KEY;
import static org.reaktivity.nukleus.kafka.internal.types.codec.message.RecordBatchFW.FIELD_OFFSET_LENGTH;
import static org.reaktivity.nukleus.kafka.internal.types.codec.message.RecordBatchFW.FIELD_OFFSET_RECORD_COUNT;
import static org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW.Builder.DEFAULT_DELTA_TYPE;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;
import java.util.zip.CRC32C;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.types.Array32FW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.RequestHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.ResponseHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.message.RecordBatchFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.message.RecordHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.message.RecordTrailerFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.produce.ProduceAck;
import org.reaktivity.nukleus.kafka.internal.types.codec.produce.ProducePartitionRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.produce.ProducePartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.produce.ProduceRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.produce.ProduceResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.produce.ProduceResponseTrailerFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.produce.ProduceTopicRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.produce.ProduceTopicResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ExtensionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaProduceBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaProduceDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaResetExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaClientProduceFactory implements StreamFactory
{
    private static final int PRODUCE_REQUEST_RECORDS_OFFSET_MAX = 512;

    private static final int KAFKA_RECORD_FRAMING = 100; // TODO

    private static final int FLAGS_CON = 0x00;
    private static final int FLAGS_FIN = 0x01;
    private static final int FLAGS_INIT = 0x02;

    private static final byte RECORD_BATCH_MAGIC = 2;
    private static final short RECORD_BATCH_ATTRIBUTES_NONE = 0;
    private static final short RECORD_BATCH_ATTRIBUTES_NO_TIMESTAMP = 0x08;
    private static final int RECORD_BATCH_PRODUCER_ID_NONE = -1;
    private static final short RECORD_BATCH_PRODUCER_EPOCH_NONE = -1;
    private static final short RECORD_BATCH_SEQUENCE_NONE = -1;
    private static final byte RECORD_ATTRIBUTES_NONE = 0;

    private static final String TRANSACTION_ID_NONE = null;
    private static final String CLIENT_ID_NONE = null;

    private static final int TIMESTAMP_NONE = 0;

    private static final int RECORD_LENGTH_MAX = 5; // varint32(max_value)

    private static final int ERROR_NONE = 0;

    private static final int SIGNAL_NEXT_REQUEST = 1;

    private static final DirectBuffer EMPTY_BUFFER = new UnsafeBuffer();
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(EMPTY_BUFFER, 0, 0);
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};
    private static final byte[] ANY_IP_ADDR = new byte[4];
    private static final Array32FW<KafkaHeaderFW> EMPTY_HEADER =
        new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
        .wrap(new UnsafeBuffer(new byte[64]), 0, 64).build();

    private static final short PRODUCE_API_KEY = 0;
    private static final short PRODUCE_API_VERSION = 3;

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
    private final KafkaDataExFW kafkaDataExRO = new KafkaDataExFW();
    private final Array32FW<KafkaHeaderFW> kafkaHeaderRO = new Array32FW<>(new  KafkaHeaderFW());

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();
    private final KafkaResetExFW.Builder kafkaResetExRW = new KafkaResetExFW.Builder();
    private final TcpBeginExFW.Builder tcpBeginExRW = new TcpBeginExFW.Builder();

    private final RequestHeaderFW.Builder requestHeaderRW = new RequestHeaderFW.Builder();
    private final ProduceRequestFW.Builder produceRequestRW = new ProduceRequestFW.Builder();
    private final ProduceTopicRequestFW.Builder topicRequestRW = new ProduceTopicRequestFW.Builder();
    private final ProducePartitionRequestFW.Builder partitionRequestRW = new ProducePartitionRequestFW.Builder();
    private final RecordBatchFW.Builder recordBatchRW = new RecordBatchFW.Builder();
    private final RecordHeaderFW.Builder recordHeaderRW = new RecordHeaderFW.Builder();
    private final RecordTrailerFW.Builder recordTrailerRW = new RecordTrailerFW.Builder();

    private final RecordHeaderFW recordHeaderRO = new RecordHeaderFW();
    private final ResponseHeaderFW responseHeaderRO = new ResponseHeaderFW();
    private final ProduceResponseFW produceResponseRO = new ProduceResponseFW();
    private final ProduceTopicResponseFW produceTopicResponseRO = new ProduceTopicResponseFW();
    private final ProducePartitionResponseFW producePartitionResponseRO = new ProducePartitionResponseFW();
    private final ProduceResponseTrailerFW produceResponseTrailerRO = new ProduceResponseTrailerFW();

    private final KafkaProduceClientEncoder encodeRecord = this::encodeRecord;
    private final KafkaProduceClientEncoder encodeRecordInit = this::encodeRecordInit;
    private final KafkaProduceClientEncoder encodeRecordContFin = this::encodeRecordContFin;

    private final KafkaProduceClientDecoder decodeProduceResponse = this::decodeProduceResponse;
    private final KafkaProduceClientDecoder decodeProduce = this::decodeProduce;
    private final KafkaProduceClientDecoder decodeProduceTopics = this::decodeProduceTopics;
    private final KafkaProduceClientDecoder decodeProduceTopic = this::decodeProduceTopic;
    private final KafkaProduceClientDecoder decodeProducePartitions = this::decodeProducePartitions;
    private final KafkaProduceClientDecoder decodeProducePartition = this::decodeProducePartition;
    private final KafkaProduceClientDecoder decodeProduceResponseTrailer = this::decodeProduceResponseTrailer;

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final int produceMaxWaitMillis;
    private final long produceRequestMaxDelay;
    private final ProduceAck produceAcks;
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
    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final LongFunction<KafkaClientRoute> supplyClientRoute;
    private final int decodeMaxBytes;
    private final int encodeMaxBytes;
    private final CRC32C crc32c;

    public KafkaClientProduceFactory(
        KafkaConfiguration config,
        RouteManager router,
        Signaler signaler,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        Long2ObjectHashMap<MessageConsumer> correlations,
        LongFunction<KafkaClientRoute> supplyClientRoute)
    {
        this.produceMaxWaitMillis = config.clientProduceMaxResponseMillis();
        this.produceRequestMaxDelay = config.clientProduceMaxRequestMillis();
        this.produceAcks = ProduceAck.valueOf(config.clientProduceAcks());
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
        this.correlations = correlations;
        this.supplyClientRoute = supplyClientRoute;
        this.decodeMaxBytes = decodePool.slotCapacity();
        this.encodeMaxBytes = Math.min(config.clientProduceMaxBytes(),
                encodePool.slotCapacity() - PRODUCE_REQUEST_RECORDS_OFFSET_MAX);
        this.crc32c = new CRC32C();
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
        assert kafkaBeginEx == null || kafkaBeginEx.kind() == KafkaBeginExFW.KIND_PRODUCE;
        final KafkaProduceBeginExFW kafkaProduceBeginEx = kafkaBeginEx != null ? kafkaBeginEx.produce() : null;

        MessageConsumer newStream = null;

        if (kafkaProduceBeginEx != null)
        {
            final String16FW beginTopic = kafkaProduceBeginEx.topic();

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
                final int partitionId = kafkaProduceBeginEx.partitionId();

                newStream = new KafkaProduceStream(
                        application,
                        routeId,
                        initialId,
                        affinity,
                        resolvedId,
                        topic,
                        partitionId)::onApplication;
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
    private interface KafkaProduceClientEncoder
    {
        int encode(
            KafkaProduceClient client,
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            int flags,
            OctetsFW payload,
            OctetsFW extension,
            int progress,
            int limit);
    }

    private int encodeRecord(
        KafkaProduceClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        int flags,
        OctetsFW payload,
        OctetsFW extension,
        int progress,
        int limit)
    {
        if (client.dataFlags == FLAGS_INIT)
        {
            client.encoder = this::encodeRecordInit;
        }

        return progress;
    }

    private int encodeRecordInit(
        KafkaProduceClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        int flags,
        OctetsFW payload,
        OctetsFW extension,
        int progress,
        int limit)
    {
        final KafkaDataExFW kafkaDataEx = extension.get(kafkaDataExRO::tryWrap);
        assert kafkaDataEx != null;
        assert kafkaDataEx.kind() == KafkaDataExFW.KIND_PRODUCE;
        final KafkaProduceDataExFW kafkaProduceDataEx = kafkaDataEx.produce();
        final long timestamp = kafkaProduceDataEx.timestamp();
        final KafkaKeyFW key = kafkaProduceDataEx.key();
        final Array32FW<KafkaHeaderFW> headers = kafkaProduceDataEx.headers();
        client.encodeableRecordBytesDeferred = kafkaProduceDataEx.deferred();

        // TODO: flow control includes headers (extension)
        if (client.encodeSlot != NO_SLOT &&
            client.encodeSlotLimit + payload.sizeof() + KAFKA_RECORD_FRAMING > encodePool.slotCapacity())
        {
            client.doEncodeRequestIfNecessary(traceId);
        }

        client.doEncodeRecordInit(traceId, timestamp, key, payload, headers);
        client.encoder = this::encodeRecordContFin;
        client.dataFlags = FLAGS_INIT;

        return progress;
    }

    private int encodeRecordContFin(
        KafkaProduceClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        int flags,
        OctetsFW payload,
        OctetsFW extension,
        int progress,
        int limit)
    {
        final int length = payload != null ? payload.sizeof() : 0;
        client.doEncodeRecordCont(traceId, payload);
        client.dataFlags = FLAGS_CON;

        progress += length;

        if ((flags & FLAGS_FIN) == FLAGS_FIN)
        {
            client.doEncodeRecordFin(traceId);
            assert progress == limit;
            client.encoder = this::encodeRecord;
            client.dataFlags = FLAGS_FIN;
        }

        return progress;
    }

    @FunctionalInterface
    private interface KafkaProduceClientDecoder
    {
        int decode(
            KafkaProduceClient client,
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            MutableDirectBuffer buffer,
            int offset,
            int progress,
            int limit);
    }

    private int decodeProduceResponse(
        KafkaProduceClient client,
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
                client.decoder = decodeProduce;
            }
        }

        return progress;
    }

    private int decodeProduce(
        KafkaProduceClient client,
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
            final ProduceResponseFW response = produceResponseRO.tryWrap(buffer, progress, limit);

            if (response != null)
            {
                progress = response.limit();

                client.decodableResponseBytes -= response.sizeof();
                assert client.decodableResponseBytes >= 0;

                client.decodableTopics = response.topicCount();
                client.decoder = decodeProduceTopics;
            }
        }

        return progress;
    }

    private int decodeProduceTopics(
        KafkaProduceClient client,
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
            client.decoder = decodeProduceResponseTrailer;
        }
        else
        {
            client.decoder = decodeProduceTopic;
        }

        return progress;
    }

    private int decodeProduceTopic(
        KafkaProduceClient client,
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
            final ProduceTopicResponseFW topicResponse = produceTopicResponseRO.tryWrap(buffer, progress, limit);
            if (topicResponse == null)
            {
                break decode;
            }

            final String16FW topic = topicResponse.topic();
            final String topicName = topic.asString();
            assert client.topic.equals(topicName);

            progress = topicResponse.limit();

            client.decodableResponseBytes -= topicResponse.sizeof();
            assert client.decodableResponseBytes >= 0;

            client.decodablePartitions = topicResponse.partitionCount();
            client.decoder = decodeProducePartitions;
        }

        return progress;
    }

    private int decodeProducePartitions(
        KafkaProduceClient client,
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

            client.decoder = decodeProduceTopics;
        }
        else
        {
            client.decoder = decodeProducePartition;
        }

        return progress;
    }

    private int decodeProducePartition(
        KafkaProduceClient client,
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
            final ProducePartitionResponseFW partition = producePartitionResponseRO.tryWrap(buffer, progress, limit);
            if (partition == null)
            {
                break decode;
            }

            final int partitionId = partition.partitionId();
            final int errorCode = partition.errorCode();

            progress = partition.limit();

            client.decodableResponseBytes -= partition.sizeof();
            assert client.decodableResponseBytes >= 0;

            client.decodablePartitions--;
            assert client.decodablePartitions >= 0;

            client.onDecodeProducePartition(traceId, authorization, errorCode, partitionId);

            client.decoder = decodeProducePartitions;
        }

        return progress;
    }

    private int decodeProduceResponseTrailer(
        KafkaProduceClient client,
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
            final ProduceResponseTrailerFW trailer = produceResponseTrailerRO.tryWrap(buffer, progress, limit);
            if (trailer != null)
            {
                progress = trailer.limit();

                client.decodableResponseBytes -= trailer.sizeof();
                assert client.decodableResponseBytes == 0;

                client.decoder = decodeProduceResponse;

                client.onDecodeResponse(traceId);
            }
        }

        return progress;
    }

    private final class KafkaProduceStream
    {
        private final MessageConsumer application;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final KafkaProduceClient client;

        private int state;

        private int initialBudget;

        KafkaProduceStream(
            MessageConsumer application,
            long routeId,
            long initialId,
            long affinity,
            long resolvedId,
            String topic,
            int partitionId)
        {
            this.application = application;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.affinity = affinity;
            this.client = new KafkaProduceClient(this, resolvedId, topic, partitionId);
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
            doApplicationWindowIfNecessary(traceId, encodeMaxBytes);

            client.doNetworkBegin(traceId, authorization, affinity);
        }

        private void onApplicationData(
            DataFW data)
        {
            final long authorization = data.authorization();
            final long budgetId = data.budgetId();
            final long traceId = data.traceId();
            final int reserved = data.reserved();
            final int flags = data.flags();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();
            int progress = payload != null ? payload.offset() : -1;
            final int limit = payload != null ? payload.limit() : -1;

            if (KafkaConfiguration.DEBUG_PRODUCE)
            {
                System.out.format("[%d] [%d] [%d] kafka client [%s[%d] %d - %d => %d\n",
                        currentTimeMillis(), currentThread().getId(),
                        initialId, client.topic, client.partitionId, initialBudget, reserved, initialBudget - reserved);
            }

            initialBudget -= reserved;

            if (initialBudget < 0)
            {
                cleanupApplication(traceId, EMPTY_OCTETS);
                client.cleanupNetwork(traceId);
            }
            else
            {
                client.encode(traceId, authorization, budgetId, reserved, flags, payload, extension, progress, limit);
            }
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
            state = KafkaState.openedReply(state);
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
            int partitionId)
        {
            if (!KafkaState.replyOpening(state))
            {
                doApplicationBegin(traceId, authorization, topic, partitionId);
            }
        }

        private void doApplicationBegin(
            long traceId,
            long authorization,
            String topic,
            int partitionId)
        {
            state = KafkaState.openingReply(state);

            router.setThrottle(replyId, this::onApplication);
            doBegin(application, routeId, replyId, traceId, authorization, affinity,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                                                        .typeId(kafkaTypeId)
                                                        .produce(p -> p.transaction(TRANSACTION_ID_NONE)
                                                                       .topic(topic)
                                                                       .partitionId(partitionId))
                                                        .build()
                                                        .sizeof()));
        }

        private void doApplicationEnd(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            //client.stream = nullIfClosed(state, client.stream);
            doEnd(application, routeId, replyId, traceId, client.authorization, EMPTY_EXTENSION);
        }

        private void doApplicationAbort(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            //client.stream = nullIfClosed(state, client.stream);
            doAbort(application, routeId, replyId, traceId, client.authorization, EMPTY_EXTENSION);
        }

        private void doApplicationWindowIfNecessary(
            long traceId,
            int initialBudgetMax)
        {
            final int credit = initialBudgetMax - initialBudget;

            if (!KafkaState.initialOpened(state) || credit > 0)
            {
                doApplicationWindow(traceId, 0L, credit);
            }
        }

        private void doApplicationWindow(
            long traceId,
            long budgetId,
            int credit)
        {
            state = KafkaState.openedInitial(state);

            if (KafkaConfiguration.DEBUG_PRODUCE)
            {
                System.out.format("[%d] [%d] [%d] kafka client [%s[%d] %d + %d => %d\n",
                        currentTimeMillis(), currentThread().getId(),
                        initialId, client.topic, client.partitionId, initialBudget, credit, initialBudget + credit);
            }

            initialBudget += credit;

            doWindow(application, routeId, initialId, traceId, client.authorization,
                    budgetId, credit, KAFKA_RECORD_FRAMING);

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
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doApplicationReset(traceId, extension);
            }
        }

        private void cleanupApplication(
            long traceId,
            Flyweight extension)
        {
            doApplicationResetIfNecessary(traceId, extension);
            doApplicationAbortIfNecessary(traceId);
        }
    }

    private final class KafkaProduceClient
    {
        private final KafkaProduceStream stream;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer network;
        private final String topic;
        private final int partitionId;
        private final KafkaClientRoute clientRoute;

        private int state;
        private int dataFlags;
        private long authorization;

        private long initialBudgetId;
        private int initialBudget;
        private int initialPadding;
        private int replyBudget;

        private int encodeSlot = NO_SLOT;
        private int encodeSlotOffset;
        private int encodeSlotLimit;
        private long encodeSlotTraceId;

        private long encodeableRecordBatchTimestamp;
        private long encodeableRecordBatchTimestampMax;
        private long encodeableRecordTimestamp;
        private int encodeableRecordHeadersBytes;
        private int encodeableRecordCount;
        private int encodeableRecordBytes;
        private int encodeableRecordBytesDeferred;
        private int encodeableRequestBytes;

        private int decodeSlot = NO_SLOT;
        private int decodeSlotOffset;
        private int decodeSlotReserved;

        private int decodableResponseBytes;
        private int decodableTopics;
        private int decodablePartitions;

        private int nextRequestId;
        private int nextResponseId;

        private KafkaProduceClientDecoder decoder;
        private KafkaProduceClientEncoder encoder;
        private int signaledRequestId;

        KafkaProduceClient(
            KafkaProduceStream stream,
            long routeId,
            String topic,
            int partitionId)
        {
            this.stream = stream;
            this.routeId = routeId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.network = router.supplyReceiver(initialId);
            this.topic = requireNonNull(topic);
            this.partitionId = partitionId;
            this.decoder = decodeProduceResponse;
            this.encoder = encodeRecord;
            this.clientRoute = supplyClientRoute.apply(routeId);
            this.encodeableRecordBatchTimestamp = TIMESTAMP_NONE;
            this.encodeableRecordBatchTimestampMax = TIMESTAMP_NONE;
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

            doNetworkWindow(traceId, 0L, decodeMaxBytes, 0);

            stream.doApplicationBeginIfNecessary(traceId, authorization, topic, partitionId);
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
                stream.doApplicationEnd(traceId);
            }
        }

        private void onNetworkAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            if (KafkaConfiguration.DEBUG)
            {
                System.out.format("[client] %s[%s] PRODUCE aborted (%d bytes)\n", topic, partitionId);
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
                System.out.format("[client] %s[%d] PRODUCE reset (%d bytes)\n", topic, partitionId);
            }

            state = KafkaState.closedInitial(state);

            cleanupNetwork(traceId);
        }

        private void onNetworkWindow(
            WindowFW window)
        {
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
                final int offset = encodeSlotOffset;
                final int limit = encodeSlotLimit;

                encodeNetwork(encodeSlotTraceId, authorization, budgetId, buffer, offset, limit);
            }
        }

        private void onNetworkSignal(
            SignalFW signal)
        {
            final long traceId = signal.traceId();
            final int signalId = signal.signalId();

            if (signalId == SIGNAL_NEXT_REQUEST)
            {
                doEncodeRequestIfNecessary(traceId);
            }
        }

        private void doNetworkBegin(
            long traceId,
            long authorization,
            long affinity)
        {
            state = KafkaState.openingInitial(state);
            correlations.put(replyId, this::onNetwork);

            Consumer<OctetsFW.Builder> extension = EMPTY_EXTENSION;

            final KafkaBrokerInfo broker = clientRoute.brokers.get(affinity);
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
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            if (encodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer encodeBuffer = encodePool.buffer(encodeSlot);
                encodeBuffer.putBytes(encodeSlotLimit, buffer, offset, limit - offset);
                encodeSlotLimit += limit - offset;
                encodeSlotTraceId = traceId;

                buffer = encodeBuffer;
                offset = encodeSlotOffset;
                limit = encodeSlotLimit;
            }

            encodeNetwork(traceId, authorization, initialBudgetId, buffer, offset, limit);
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

        private void encode(
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            int flags,
            OctetsFW payload,
            OctetsFW extension,
            int progress,
            int limit)
        {
            KafkaProduceClientEncoder previous = null;
            dataFlags = flags & FLAGS_INIT;

            while (progress <= limit && previous != encoder)
            {
                previous = encoder;
                progress = encoder.encode(this, traceId, authorization, budgetId, reserved, flags, payload, extension,
                    progress, limit);
            }
        }

        private void doEncodeRecordInit(
            long traceId,
            long timestamp,
            KafkaKeyFW key,
            OctetsFW value,
            Array32FW<KafkaHeaderFW> headers)
        {
            encodeableRecordTimestamp = timestamp;
            if (encodeableRecordBatchTimestamp == TIMESTAMP_NONE)
            {
                encodeableRecordBatchTimestamp = timestamp;
            }

            final MutableDirectBuffer encodeBuffer = writeBuffer;
            final int encodeLimit = writeBuffer.capacity();
            int encodeProgress = 0;

            final int headersCount = headers.fieldCount();
            final RecordTrailerFW recordTrailer = recordTrailerRW.wrap(encodeBuffer, 0, encodeLimit)
                                                                 .headerCount(headersCount)
                                                                 .build();
            final int recordTrailerSize = recordTrailer.limit();

            final int timestampDelta = (int) (timestamp - encodeableRecordBatchTimestamp);
            RecordHeaderFW recordHeader = recordHeaderRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                    .length(Integer.MAX_VALUE)
                    .attributes(RECORD_ATTRIBUTES_NONE)
                    .timestampDelta(timestampDelta)
                    .offsetDelta(encodeableRecordCount)
                    .keyLength(key.length())
                    .key(key.value())
                    .valueLength(value != null ? value.sizeof() : -1)
                    .build();

            final int valueSize = value != null ? value.sizeof() : 0;
            final int recordHeaderSize = recordHeader.limit();
            final int headerSize = headers.items().capacity();
            final int recordSize = recordHeaderSize + valueSize + encodeableRecordBytesDeferred + recordTrailerSize +
                        headerSize - RECORD_LENGTH_MAX;

            recordHeader = recordHeaderRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                                         .length(recordSize)
                                         .attributes(RECORD_ATTRIBUTES_NONE)
                                         .timestampDelta(timestampDelta)
                                         .offsetDelta(encodeableRecordCount)
                                         .keyLength(key.length())
                                         .key(key.value())
                                         .valueLength(value != null ? value.sizeof() : -1)
                                         .build();

            encodeProgress = recordHeader.limit();

            if (encodeSlot == NO_SLOT)
            {
                encodeSlot = encodePool.acquire(initialId);
                encodeSlotOffset = PRODUCE_REQUEST_RECORDS_OFFSET_MAX;
                encodeSlotLimit = encodeSlotOffset;
            }

            assert encodeSlot != NO_SLOT;
            final MutableDirectBuffer encodeSlotBuffer = encodePool.buffer(encodeSlot);

            encodeSlotBuffer.putBytes(encodeSlotLimit, encodeBuffer, 0, encodeProgress);
            encodeSlotLimit += encodeProgress;
            encodeableRecordBytes += encodeProgress;

            if (headersCount > 0)
            {
                encodeableRecordHeadersBytes = headers.sizeof();

                //TODO: check if this requires increasing encodeSlotLimit
                final int encodeSlotMaxLimit = encodePool.slotCapacity() - encodeableRecordHeadersBytes;
                encodeSlotBuffer.putBytes(encodeSlotMaxLimit, headers.buffer(), headers.offset(), encodeableRecordHeadersBytes);
            }
        }

        private void doEncodeRecordCont(
            long traceId,
            OctetsFW value)
        {
            if (value != null)
            {
                assert encodeSlot != NO_SLOT;
                final MutableDirectBuffer encodeSlotBuffer = encodePool.buffer(encodeSlot);

                final int length = value.sizeof();
                encodeSlotBuffer.putBytes(encodeSlotLimit,  value.buffer(), value.offset(), length);
                encodeSlotLimit += length;
                encodeableRecordBytes += length;
            }
        }

        private void doEncodeRecordFin(
            long traceId)
        {
            assert encodeSlot != NO_SLOT;
            final MutableDirectBuffer encodeSlotBuffer = encodePool.buffer(encodeSlot);

            final MutableDirectBuffer encodeBuffer = writeBuffer;
            final int encodeLimit = writeBuffer.capacity();
            int encodeProgress = 0;

            Array32FW<KafkaHeaderFW> headers = EMPTY_HEADER;
            if (encodeableRecordHeadersBytes > 0)
            {
                final int encodeSlotMaxLimit = encodePool.slotCapacity();
                headers = kafkaHeaderRO.wrap(encodeSlotBuffer, encodeSlotMaxLimit - encodeableRecordHeadersBytes,
                    encodeSlotMaxLimit);
            }
            final int headersCount = headers.fieldCount();
            final RecordTrailerFW recordTrailer = recordTrailerRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                    .headerCount(headersCount)
                    .build();

            encodeProgress = recordTrailer.limit();

            if (headersCount > 0)
            {
                final DirectBuffer headerItems = headers.items();
                final int headerItemsSize = headerItems.capacity();

                encodeBuffer.putBytes(encodeProgress, headerItems, 0, headerItemsSize);
                encodeProgress += headerItemsSize;
            }

            encodeSlotBuffer.putBytes(encodeSlotLimit, encodeBuffer, 0, encodeProgress);
            encodeSlotLimit += encodeProgress;
            encodeableRecordBytes += encodeProgress;

            encodeableRecordCount++;

            encodeableRecordBatchTimestampMax = Math.max(encodeableRecordBatchTimestamp, encodeableRecordTimestamp);

            doSignalNextRequestIfNecessary(traceId);
        }

        private void doSignalNextRequestIfNecessary(
            long traceId)
        {
            if (signaledRequestId <= nextRequestId &&
                nextRequestId == nextResponseId &&
                encodeSlot != NO_SLOT)
            {
                if (produceRequestMaxDelay == 0)
                {
                    signaler.signalNow(routeId, initialId, SIGNAL_NEXT_REQUEST);
                }
                else
                {
                    signaler.signalAt(currentTimeMillis() + produceRequestMaxDelay, routeId, initialId, SIGNAL_NEXT_REQUEST);
                }
                signaledRequestId = nextRequestId + 1;
            }
        }

        private void doEncodeRequestIfNecessary(
            long traceId)
        {
            if (nextRequestId == nextResponseId && encodeSlot != NO_SLOT)
            {
                doEncodeProduceRequest(traceId);
            }
        }

        private void doEncodeProduceRequest(
            long traceId)
        {
            final MutableDirectBuffer encodeBuffer = writeBuffer;
            final int encodeOffset = 0;
            final int encodeLimit = encodeBuffer.capacity();

            int encodeProgress = encodeOffset;

            final RequestHeaderFW requestHeader = requestHeaderRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                    .length(0)
                    .apiKey(PRODUCE_API_KEY)
                    .apiVersion(PRODUCE_API_VERSION)
                    .correlationId(0)
                    .clientId(CLIENT_ID_NONE)
                    .build();

            encodeProgress = requestHeader.limit();

            final ProduceRequestFW produceRequest = produceRequestRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                    .transactionalId(TRANSACTION_ID_NONE)
                    .acks(a -> a.set(produceAcks))
                    .timeout(produceMaxWaitMillis)
                    .topicCount(1)
                    .build();

            encodeProgress = produceRequest.limit();

            final ProduceTopicRequestFW topicRequest = topicRequestRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                    .topic(topic)
                    .partitionCount(1)
                    .build();

            encodeProgress = topicRequest.limit();

            final int recordBatchLength = FIELD_OFFSET_RECORD_COUNT - FIELD_OFFSET_LENGTH + encodeableRecordBytes;
            final int recordSetLength = FIELD_OFFSET_LENGTH + BitUtil.SIZE_OF_INT + recordBatchLength;

            final ProducePartitionRequestFW partitionRequest =
                    partitionRequestRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                    .partitionId(partitionId)
                    .recordSetLength(recordSetLength)
                    .build();

            encodeProgress = partitionRequest.limit();

            final int crcOffset = encodeProgress - encodeOffset + RecordBatchFW.FIELD_OFFSET_CRC;
            final int crcLimit = encodeProgress - encodeOffset + RecordBatchFW.FIELD_OFFSET_ATTRIBUTES;

            final short attributes = encodeableRecordBatchTimestampMax == 0L
                    ? RECORD_BATCH_ATTRIBUTES_NO_TIMESTAMP
                    : RECORD_BATCH_ATTRIBUTES_NONE;

            final RecordBatchFW recordBatch = recordBatchRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                    .baseOffset(0)
                    .length(recordBatchLength)
                    .leaderEpoch(-1)
                    .magic(RECORD_BATCH_MAGIC)
                    .crc(0)
                    .attributes(attributes)
                    .lastOffsetDelta(encodeableRecordCount - 1)
                    .firstTimestamp(encodeableRecordBatchTimestamp)
                    .maxTimestamp(encodeableRecordBatchTimestampMax)
                    .producerId(RECORD_BATCH_PRODUCER_ID_NONE)
                    .producerEpoch(RECORD_BATCH_PRODUCER_EPOCH_NONE)
                    .baseSequence(RECORD_BATCH_SEQUENCE_NONE)
                    .recordCount(encodeableRecordCount)
                    .build();

            encodeProgress = recordBatch.limit();

            assert encodeProgress <= PRODUCE_REQUEST_RECORDS_OFFSET_MAX;

            final int encodeSizeOf = encodeProgress - encodeOffset;
            final int requestId = nextRequestId++;
            final int requestSize = encodeSizeOf - FIELD_OFFSET_API_KEY + encodeableRecordBytes;

            requestHeaderRW.wrap(encodeBuffer, requestHeader.offset(), requestHeader.limit())
                    .length(requestSize)
                    .apiKey(requestHeader.apiKey())
                    .apiVersion(requestHeader.apiVersion())
                    .correlationId(requestId)
                    .clientId(requestHeader.clientId().asString())
                    .build();

            if (KafkaConfiguration.DEBUG)
            {
                System.out.format("[client] %s[%d] PRODUCE\n", topic, partitionId);
            }

            encodeableRecordCount = 0;
            encodeableRecordBytes = 0;
            encodeableRecordBatchTimestamp = TIMESTAMP_NONE;

            assert encodeableRequestBytes == 0;
            encodeableRequestBytes = encodeSizeOf + encodeSlotLimit - encodeSlotOffset;

            assert encodeSlot != NO_SLOT;
            final MutableDirectBuffer encodeSlotBuffer = encodePool.buffer(encodeSlot);

            encodeSlotOffset -= encodeSizeOf;
            assert encodeSlotOffset >= 0;
            encodeSlotBuffer.putBytes(encodeSlotOffset, encodeBuffer, encodeOffset, encodeSizeOf);

            final ByteBuffer encodeSlotByteBuffer = encodePool.byteBuffer(encodeSlot);
            final int encodeSlotBytePosition = encodeSlotByteBuffer.position();
            encodeSlotByteBuffer.limit(encodeSlotBytePosition + encodeSlotLimit);
            encodeSlotByteBuffer.position(encodeSlotBytePosition + encodeSlotOffset + crcLimit);

            final CRC32C crc = crc32c;
            crc.reset();
            crc.update(encodeSlotByteBuffer);
            encodeSlotBuffer.putInt(encodeSlotOffset + crcOffset, (int) crc.getValue(), BIG_ENDIAN);

            doNetworkData(traceId, EMPTY_BUFFER, 0, 0);
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
            final int maxRequestLength = Math.min(maxLength, encodeableRequestBytes);
            final int length = Math.max(Math.min(initialBudget - initialPadding, maxRequestLength), 0);

            if (length > 0)
            {
                final int reserved = length + initialPadding;

                initialBudget -= reserved;

                assert initialBudget >= 0 : String.format("%d >= 0", initialBudget);

                doData(network, routeId, initialId, traceId, authorization, budgetId,
                       reserved, buffer, offset, length, EMPTY_OCTETS);

                encodeableRequestBytes -= length;
                assert encodeableRequestBytes >= 0;

                if (KafkaConfiguration.DEBUG_PRODUCE)
                {
                    System.out.format("[%d] [%d] [%d] kafka client [%s[%d] encodeableRequestBytes %d\n",
                            currentTimeMillis(), currentThread().getId(),
                            initialId, topic, partitionId, encodeableRequestBytes);
                }
            }

            final int remaining = maxLength - length;
            if (remaining > 0)
            {
                assert encodeSlot != NO_SLOT;

                if (encodeableRequestBytes == 0)
                {
                    encodeSlotOffset = PRODUCE_REQUEST_RECORDS_OFFSET_MAX;
                }
                final MutableDirectBuffer encodeBuffer = encodePool.buffer(encodeSlot);
                encodeBuffer.putBytes(encodeSlotOffset, buffer, offset + length, remaining);
                encodeSlotLimit = encodeSlotOffset + remaining;
            }
            else
            {
                cleanupEncodeSlotIfNecessary();

                if (KafkaState.initialClosing(state))
                {
                    doNetworkEnd(traceId, authorization);
                }
            }

            if (produceAcks == ProduceAck.NONE && length > 0 && encodeableRequestBytes == 0)
            {
                onDecodeResponse(traceId);
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
            KafkaProduceClientDecoder previous = null;
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
                    stream.doApplicationEnd(traceId);
                }
                else if (reserved > 0)
                {
                    doNetworkWindow(traceId, budgetId, reserved, 0);
                }
            }
        }

        private void onDecodeProducePartition(
            long traceId,
            long authorization,
            int errorCode,
            int partitionId)
        {
            switch (errorCode)
            {
            case ERROR_NONE:
                assert partitionId == this.partitionId;
                break;
            default:
                final KafkaResetExFW resetEx = kafkaResetExRW.wrap(extBuffer, 0, extBuffer.capacity())
                                                             .typeId(kafkaTypeId)
                                                             .error(errorCode)
                                                             .build();
                stream.doApplicationResetIfNecessary(traceId, resetEx);
                doNetworkEnd(traceId, authorization);
                break;
            }
        }

        private void onDecodeResponse(
            long traceId)
        {
            nextResponseId++;

            stream.doApplicationWindowIfNecessary(traceId, encodeMaxBytes - encodeSlotLimit);

            if (encodeSlot != NO_SLOT)
            {
                signaler.signalNow(routeId, initialId, SIGNAL_NEXT_REQUEST);
            }
        }

        private void cleanupNetwork(
            long traceId)
        {
            doNetworkResetIfNecessary(traceId);
            doNetworkAbortIfNecessary(traceId);

            stream.cleanupApplication(traceId, EMPTY_OCTETS);
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
                encodeSlotLimit = 0;
                encodeSlotTraceId = 0;
            }
        }
    }
}
