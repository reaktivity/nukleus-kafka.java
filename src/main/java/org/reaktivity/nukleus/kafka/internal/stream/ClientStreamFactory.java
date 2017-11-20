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
package org.reaktivity.nukleus.kafka.internal.stream;

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2LongHashMap.LongIterator;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.function.PartitionProgressHandler;
import org.reaktivity.nukleus.kafka.internal.function.PartitionResponseConsumer;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.Varint64FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.RequestHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.ResponseHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordBatchFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ClientStreamFactory implements StreamFactory
{
    private static final short FETCH_API_VERSION = 0x05;
    private static final short FETCH_API_KEY = 0x01;

    private final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();
    private final KafkaBeginExFW beginExRO = new KafkaBeginExFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final KafkaDataExFW.Builder dataExRW = new KafkaDataExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final RequestHeaderFW.Builder requestRW = new RequestHeaderFW.Builder();
    private final FetchRequestFW.Builder fetchRequestRW = new FetchRequestFW.Builder();
    private final TopicRequestFW.Builder topicRequestRW = new TopicRequestFW.Builder();
    private final PartitionRequestFW.Builder partitionRequestRW = new PartitionRequestFW.Builder();
    private final OctetsFW.Builder payloadRW = new OctetsFW.Builder();

    private final ResponseHeaderFW responseRO = new ResponseHeaderFW();
    private final FetchResponseFW fetchResponseRO = new FetchResponseFW();
    private final TopicResponseFW topicResponseRO = new TopicResponseFW();
    private final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    private final RecordSetFW recordSetRO = new RecordSetFW();
    private final RecordBatchFW recordBatchRO = new RecordBatchFW();
    private final RecordFW recordRO = new RecordFW();

    private final RouteManager router;
    private final LongSupplier supplyStreamId;
    private final LongSupplier supplyCorrelationId;
    private final BufferPool bufferPool;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer encodeBuffer;

    private final Long2ObjectHashMap<NetworkConnectionPool.NetworkConnection> correlations;
    private final Map<String, Long2ObjectHashMap<NetworkConnectionPool>> connectionPools;

    public ClientStreamFactory(
        KafkaConfiguration configuration,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<NetworkConnectionPool.NetworkConnection> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = supplyCorrelationId;
        this.correlations = requireNonNull(correlations);
        this.connectionPools = new LinkedHashMap<String, Long2ObjectHashMap<NetworkConnectionPool>>();
        this.encodeBuffer = new UnsafeBuffer(new byte[bufferPool.slotCapacity()]);
    }

    @Override
    public MessageConsumer newStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        BeginFW begin,
        MessageConsumer applicationThrottle)
    {
        final long authorization = begin.authorization();
        final long acceptRef = begin.sourceRef();

        final OctetsFW extension = beginRO.extension();

        MessageConsumer newStream = null;

        if (extension.sizeof() > 0)
        {
            final KafkaBeginExFW beginEx = extension.get(beginExRO::wrap);
            String topicName = beginEx.topicName().asString();

            final RouteFW route = resolveRoute(authorization, acceptRef, topicName);
            if (route != null)
            {
                final long applicationId = begin.streamId();
                final String networkName = route.target().asString();
                final long networkRef = route.targetRef();

                Long2ObjectHashMap<NetworkConnectionPool> connectionPoolsByRef =
                    connectionPools.computeIfAbsent(networkName, this::newConnectionPoolsByRef);

                NetworkConnectionPool connectionPool = connectionPoolsByRef.computeIfAbsent(networkRef, ref ->
                    new NetworkConnectionPool(networkName, ref));

                newStream = new ClientAcceptStream(applicationThrottle, applicationId, connectionPool)::handleStream;
            }
        }

        return newStream;
    }

    private Long2ObjectHashMap<NetworkConnectionPool> newConnectionPoolsByRef(
        String networkName)
    {
        return new Long2ObjectHashMap<>();
    }

    private MessageConsumer newConnectReplyStream(
        BeginFW begin,
        MessageConsumer networkReplyThrottle)
    {
        final long networkReplyId = begin.streamId();
        final long networkReplyRef = begin.sourceRef();
        final long networkReplyCorrelationId = begin.correlationId();

        MessageConsumer handleStream = null;
        if (networkReplyRef == 0L)
        {
            final NetworkConnectionPool.NetworkConnection connection = correlations.remove(networkReplyCorrelationId);
            if (connection != null)
            {
                handleStream = connection.onCorrelated(networkReplyThrottle, networkReplyId);
            }
        }

        return handleStream;
    }

    private RouteFW resolveRoute(
        long authorization,
        long sourceRef,
        String topicName)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            final OctetsFW extension = route.extension();
            Predicate<String> topicMatch = s -> false;
            if (extension.sizeof() > 0)
            {
                final KafkaRouteExFW routeEx = extension.get(routeExRO::wrap);
                topicMatch = routeEx.topicName().asString()::equals;
            }
            return route.sourceRef() == sourceRef && topicMatch.test(topicName);
        };

        return router.resolve(authorization, filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
        final int msgTypeId,
        final DirectBuffer buffer,
        final int index,
        final int length)
    {
        assert msgTypeId == RouteFW.TYPE_ID;
        return routeRO.wrap(buffer, index, index + length);
    }

    private void doBegin(
        final MessageConsumer target,
        final long targetId,
        final long targetRef,
        final long correlationId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .source("kafka")
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        final MessageConsumer target,
        final long targetId,
        final OctetsFW payload)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .payload(p -> p.set(payload.buffer(), payload.offset(), payload.sizeof()))
                .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    @SuppressWarnings("unused")
    private void doEnd(
        final MessageConsumer target,
        final long targetId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        final MessageConsumer target,
        final long targetId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .build();

        target.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int credit,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .credit(credit)
                .padding(padding)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .streamId(throttleId)
               .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doKafkaBegin(
        final MessageConsumer target,
        final long targetId,
        final long targetRef,
        final long correlationId,
        final OctetsFW extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .source("kafka")
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(b -> b.set(extension))
                .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doKafkaData(
        final MessageConsumer target,
        final long targetId,
        final OctetsFW payload,
        final LongIterator fetchOffsets)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .payload(p -> p.set(payload.buffer(), payload.offset(), payload.sizeof()))
                .extension(e -> e.set(visitKafkaDataEx(fetchOffsets)))
                .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private Flyweight.Builder.Visitor visitKafkaDataEx(
        LongIterator fetchOffsets)
    {
        return (b, o, l) ->
        {
            return dataExRW.wrap(b, o, l)
                           .fetchOffsets(a ->
                           {
                               while (fetchOffsets.hasNext())
                               {
                                   a.item(p -> p.set(fetchOffsets.nextValue()));
                               }
                           })
                           .build()
                           .sizeof();
        };
    }

    private final class ClientAcceptStream
    {
        private final MessageConsumer applicationThrottle;
        private final long applicationId;
        private final NetworkConnectionPool networkPool;
        private final Long2LongHashMap fetchOffsets;

        private MessageConsumer applicationReply;
        private long applicationReplyId;
        private int applicationReplyBudget;
        private int applicationReplyPadding;

        private long networkAttachId;

        private MessageConsumer streamState;

        private ClientAcceptStream(
            MessageConsumer applicationThrottle,
            long applicationId,
            NetworkConnectionPool networkPool)
        {
            this.applicationThrottle = applicationThrottle;
            this.applicationId = applicationId;
            this.networkPool = networkPool;
            this.fetchOffsets = new Long2LongHashMap(-1L);
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                doReset(applicationThrottle, applicationId);
            }
        }

        private void afterBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                doReset(applicationThrottle, applicationId);
                networkPool.doDetach(networkAttachId, fetchOffsets);
                break;
            case EndFW.TYPE_ID:
                // accept reply stream is allowed to outlive accept stream, so ignore END
                break;
            case AbortFW.TYPE_ID:
                doAbort(applicationReply, applicationReplyId);
                networkPool.doDetach(networkAttachId, fetchOffsets);
                break;
            default:
                doReset(applicationThrottle, applicationId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final String applicationName = begin.source().asString();
            final long applicationCorrelationId = begin.correlationId();
            final OctetsFW extension = begin.extension();

            if (extension.sizeof() == 0)
            {
                doReset(applicationThrottle, applicationId);
            }
            else
            {
                final KafkaBeginExFW beginEx = extension.get(beginExRO::wrap);

                final String replyName = applicationName;
                final long newReplyId = supplyStreamId.getAsLong();
                final MessageConsumer newReply = router.supplyTarget(replyName);

                final String topicName = beginEx.topicName().asString();
                final ArrayFW<Varint64FW> fetchOffsets = beginEx.fetchOffsets();

                this.fetchOffsets.clear();
                fetchOffsets.forEach(v -> this.fetchOffsets.put(this.fetchOffsets.size(), v.value()));

                final long newNetworkAttachId =
                        networkPool.doAttach(topicName, this.fetchOffsets, this::onPartitionResponse, this::writeableBytes);

                doKafkaBegin(newReply, newReplyId, 0L, applicationCorrelationId, extension);
                router.setThrottle(applicationName, newReplyId, this::handleThrottle);

                doWindow(applicationThrottle, applicationId, 0, 0);

                this.networkAttachId = newNetworkAttachId;
                this.streamState = this::afterBegin;
                this.applicationReply = newReply;
                this.applicationReplyId = newReplyId;
            }
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                handleWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleWindow(
            final WindowFW window)
        {
            applicationReplyBudget += window.credit();
            applicationReplyPadding = window.padding();

            networkPool.doFlush(networkAttachId);
        }

        private void handleReset(
            ResetFW reset)
        {
            networkPool.doDetach(networkAttachId, fetchOffsets);
            doReset(applicationThrottle, applicationId);
        }

        private void onPartitionResponse(
            DirectBuffer buffer,
            int offset,
            int length,
            PartitionProgressHandler progressHandler)
        {
            final int maxLimit = offset + length;

            final PartitionResponseFW partition = partitionResponseRO.wrap(buffer, offset, maxLimit);
            final int partitionId = partition.partitionId();

            // BUG: generated limit() ignores fixed width fields after dynamic width fields
            final RecordBatchFW records = recordBatchRO.wrap(buffer, partition.limit() + BitUtil.SIZE_OF_INT, maxLimit);
            long nextFetchAt = records.firstOffset();
            long lastFetchAt = nextFetchAt + records.recordCount();
            long firstFetchAt = fetchOffsets.get(partitionId);
            int nextRecordAt = records.limit();

            while (nextFetchAt < lastFetchAt && nextRecordAt < maxLimit)
            {
                final RecordFW record = recordRO.wrap(buffer, nextRecordAt, maxLimit);
                final int headerCount = record.headerCount();

                // BUG: generated limit() ignores fixed width fields after dynamic width fields
                int nextHeaderAt = record.limit() + BitUtil.SIZE_OF_INT;

                for (int headerIndex = 0; headerIndex < headerCount; headerIndex++)
                {
                    final HeaderFW headerRO = new HeaderFW();
                    final HeaderFW header = headerRO.wrap(buffer, nextHeaderAt, maxLimit);
                    nextHeaderAt = header.limit();
                }

                nextRecordAt = nextHeaderAt;
                nextFetchAt++;

                if (nextFetchAt > firstFetchAt)
                {
                    final OctetsFW value = record.value();

                    if (applicationReplyBudget < value.sizeof() + applicationReplyPadding)
                    {
                        break;
                    }

                    this.fetchOffsets.put(partitionId, nextFetchAt);

                    doKafkaData(applicationReply, applicationReplyId, value, fetchOffsets.values().iterator());
                    applicationReplyBudget -= value.sizeof() + applicationReplyPadding;
                }
            }


            progressHandler.handle(partitionId, firstFetchAt, nextFetchAt);
        }

        private int writeableBytes()
        {
            return applicationReplyBudget - applicationReplyPadding;
        }
    }

    private final class NetworkTopic
    {
        private final String topicName;
        private final Set<PartitionResponseConsumer> recordConsumers;
        private final Set<IntSupplier> windowSuppliers;
        private final NavigableSet<NetworkTopicPartition> partitions;
        private final NetworkTopicPartition candidate;
        private final PartitionProgressHandler progressHandler;

        @Override
        public String toString()
        {
            return String.format("topicName=%s, partitions=%s", topicName, partitions);
        }

        private NetworkTopic(
            String topicName)
        {
            this.topicName = topicName;
            this.recordConsumers = new HashSet<>();
            this.windowSuppliers = new HashSet<>();
            this.partitions = new TreeSet<>();
            this.candidate = new NetworkTopicPartition();
            this.progressHandler = this::handleProgress;
        }

        private void doAttach(
            Long2LongHashMap fetchOffsets,
            PartitionResponseConsumer consumeRecords,
            IntSupplier supplyWindow)
        {
            recordConsumers.add(consumeRecords);
            windowSuppliers.add(supplyWindow);
            candidate.id = -1;

            final LongIterator iterator = fetchOffsets.values().iterator();
            while (iterator.hasNext())
            {
                candidate.id++;
                candidate.offset = iterator.nextValue();

                NetworkTopicPartition partition = partitions.floor(candidate);
                if (partition == null || partition.id != candidate.id)
                {
                    partition = new NetworkTopicPartition();
                    partition.id = candidate.id;
                    partition.offset = candidate.offset;

                    partitions.add(partition);
                }

                partition.refs++;
            }
        }

        private void doDetach(
            Long2LongHashMap fetchOffsets,
            PartitionResponseConsumer consumeRecords,
            IntSupplier supplyWindow)
        {
            recordConsumers.remove(consumeRecords);
            windowSuppliers.remove(supplyWindow);
            candidate.id = -1;

            final LongIterator iterator = fetchOffsets.values().iterator();
            while (iterator.hasNext())
            {
                candidate.id++;
                candidate.offset = iterator.nextValue();

                NetworkTopicPartition partition = partitions.floor(candidate);
                if (partition != null)
                {
                    partition.refs--;

                    if (partition.refs == 0)
                    {
                        partitions.remove(partition);
                    }
                }
            }
        }

        private void onPartitionResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            // TODO: eliminate iterator allocation
            for (PartitionResponseConsumer recordsConsumer : recordConsumers)
            {
                recordsConsumer.accept(buffer, index, index + length, progressHandler);
            }

            partitions.removeIf(p -> p.refs == 0);
        }

        private int maximumWritableBytes()
        {
            // TODO: eliminate iterator allocation
            int writeableBytes = 0;
            for (IntSupplier supplyWindow : windowSuppliers)
            {
                writeableBytes = Math.max(writeableBytes, supplyWindow.getAsInt());
            }

            return writeableBytes;
        }

        private void handleProgress(
            int partitionId,
            long firstOffset,
            long nextOffset)
        {
            candidate.id = partitionId;
            candidate.offset = firstOffset;
            NetworkTopicPartition first = partitions.floor(candidate);
            assert first != null;
            assert first.offset == firstOffset;
            first.refs--;

            candidate.offset = nextOffset;
            NetworkTopicPartition next = partitions.floor(candidate);
            if (next == null || next.offset != nextOffset)
            {
                next = new NetworkTopicPartition();
                next.id = partitionId;
                next.offset = nextOffset;
                partitions.add(next);
            }
            next.refs++;
        }
    }

    private static final class NetworkTopicPartition implements Comparable<NetworkTopicPartition>
    {
        private int id;
        private long offset;
        private int refs;

        @Override
        public int compareTo(
            NetworkTopicPartition that)
        {
            int comparison = this.id - that.id;

            if (comparison == 0)
            {
                comparison = (int)(((this.offset - that.offset) >> 32) & 0xFFFF_FFFF);
            }

            if (comparison == 0)
            {
                comparison = (int)((this.offset - that.offset) & 0xFFFF_FFFF);
            }

            return comparison;
        }

        @Override
        public String toString()
        {
            return String.format("id=%d, offset=%d, refs=%d", id, offset, refs);
        }
    }

    final class NetworkConnectionPool
    {
        private final String networkName;
        private final long networkRef;
        private final NetworkConnection connection;

        private NetworkConnectionPool(
            String networkName,
            long networkRef)
        {
            this.networkName = networkName;
            this.networkRef = networkRef;
            this.connection = new NetworkConnection();
        }

        private long doAttach(
            String topicName,
            Long2LongHashMap fetchOffsets,
            PartitionResponseConsumer consumeRecords,
            IntSupplier supplyWindow)
        {
            // TODO: connection pool size > 1
            final int connectionId = 0;
            final int connectionAttachId = connection.doAttach(topicName, fetchOffsets, consumeRecords, supplyWindow);

            return ((long) connectionId) << 32 | connectionAttachId;
        }

        private void doFlush(
            long connectionPoolAttachId)
        {
            int connectionId = (int)((connectionPoolAttachId >> 32) & 0xFFFF_FFFF);

            // TODO: connection pool size > 1
            assert connectionId == 0;

            connection.doFlush();
        }

        private void doDetach(
            long connectionPoolAttachId,
            Long2LongHashMap fetchOffsets)
        {
            int connectionId = (int)((connectionPoolAttachId >> 32) & 0xFFFF_FFFF);
            int connectionAttachId = (int)(connectionPoolAttachId & 0xFFFF_FFFF);

            // TODO: connection pool size > 1
            assert connectionId == 0;

            connection.doDetach(connectionAttachId, fetchOffsets);
        }

        final class NetworkConnection
        {
            private final MessageConsumer networkTarget;
            private final Map<String, NetworkTopic> topicsByName;
            private final Int2ObjectHashMap<Consumer<Long2LongHashMap>> detachersById;

            private int nextAttachId;
            private int nextRequestId;
            private int nextResponseId;

            private long networkId;

            private int networkRequestBudget;
            private int networkRequestPadding;

            private int networkResponseBudget;
            private int networkResponsePadding;

            private MessageConsumer networkReplyThrottle;
            private long networkReplyId;

            private MessageConsumer streamState;

            private int networkSlot = NO_SLOT;
            private int networkSlotOffset;

            private NetworkConnection()
            {
                this.networkTarget = router.supplyTarget(networkName);
                this.topicsByName = new LinkedHashMap<>();
                this.detachersById = new Int2ObjectHashMap<>();
            }

            @Override
            public String toString()
            {
                return String.format("[budget=%d, paddng=%d]", networkRequestBudget, networkRequestPadding);
            }

            private MessageConsumer onCorrelated(
                MessageConsumer networkReplyThrottle,
                long networkReplyId)
            {
                this.networkReplyThrottle = networkReplyThrottle;
                this.networkReplyId = networkReplyId;
                this.streamState = this::beforeBegin;

                return this::handleStream;
            }

            private int doAttach(
                String topicName,
                Long2LongHashMap fetchOffsets,
                PartitionResponseConsumer consumeRecords,
                IntSupplier supplyWindow)
            {
                final NetworkTopic topic = topicsByName.computeIfAbsent(topicName, NetworkTopic::new);
                topic.doAttach(fetchOffsets, consumeRecords, supplyWindow);

                final int newAttachId = nextAttachId++;

                detachersById.put(newAttachId, f -> topic.doDetach(f, consumeRecords, supplyWindow));

                doFetchIfNotInFlight();

                return newAttachId;
            }

            private void doFlush()
            {
                doFetchIfNotInFlight();
            }

            private void doDetach(
                int attachId,
                Long2LongHashMap fetchOffsets)
            {
                final Consumer<Long2LongHashMap> detacher = detachersById.remove(attachId);
                if (detacher != null)
                {
                    detacher.accept(fetchOffsets);
                }
            }

            private void doFetchIfNotInFlight()
            {
                if (nextRequestId == nextResponseId)
                {
                    doBeginIfNotConnected();

                    if (networkRequestBudget > networkRequestPadding)
                    {
                        final int encodeOffset = 512;
                        int encodeLimit = encodeOffset;

                        RequestHeaderFW request = requestRW.wrap(encodeBuffer, encodeLimit, encodeBuffer.capacity())
                                .size(0)
                                .apiKey(FETCH_API_KEY)
                                .apiVersion(FETCH_API_VERSION)
                                .correlationId(0)
                                .clientId((String) null)
                                .build();

                        encodeLimit = request.limit();

                        FetchRequestFW fetchRequest = fetchRequestRW.wrap(encodeBuffer, encodeLimit, encodeBuffer.capacity())
                                .maxWaitTimeMillis(500)
                                .minBytes(1)
                                .maxBytes(0)
                                .isolationLevel((byte) 0)
                                .topicCount(0)
                                .build();

                        encodeLimit = fetchRequest.limit();

                        int maxFetchBytes = 0;
                        int topicCount = 0;
                        for (String topicName : topicsByName.keySet())
                        {
                            TopicRequestFW topicRequest = topicRequestRW.wrap(encodeBuffer, encodeLimit, encodeBuffer.capacity())
                                    .name(topicName)
                                    .partitionCount(0)
                                    .build();

                            // BUG: generated limit() ignores fixed width fields after dynamic width fields
                            encodeLimit = topicRequest.limit() + BitUtil.SIZE_OF_INT;

                            NetworkTopic topic = topicsByName.get(topicName);
                            int maxPartitionBytes = topic.maximumWritableBytes();

                            maxFetchBytes += maxPartitionBytes;

                            int partitionCount = 0;
                            int partitionId = -1;
                            // TODO: eliminate iterator allocation
                            for (NetworkTopicPartition partition : topic.partitions)
                            {
                                if (partitionId < partition.id)
                                {
                                    PartitionRequestFW partitionRequest = partitionRequestRW
                                        .wrap(encodeBuffer, encodeLimit, encodeBuffer.capacity())
                                        .partitionId(partition.id)
                                        .fetchOffset(partition.offset)
                                        .maxBytes(maxPartitionBytes)
                                        .build();

                                    encodeLimit = partitionRequest.limit();
                                    partitionId = partition.id;
                                    partitionCount++;
                                }
                            }

                            // BUG: generated limit() ignores fixed width fields after dynamic width fields
                            topicRequestRW.wrap(encodeBuffer, topicRequest.offset(), topicRequest.limit() + BitUtil.SIZE_OF_INT)
                                          .name(topicRequest.name())
                                          .partitionCount(partitionCount)
                                          .build();
                            topicCount++;
                        }

                        fetchRequestRW.wrap(encodeBuffer, fetchRequest.offset(), fetchRequest.limit())
                                      .maxWaitTimeMillis(fetchRequest.maxWaitTimeMillis())
                                      .minBytes(fetchRequest.minBytes())
                                      .maxBytes(maxFetchBytes)
                                      .isolationLevel(fetchRequest.isolationLevel())
                                      .topicCount(topicCount)
                                      .build();

                        // TODO: stream large requests in multiple DATA frames as needed
                        if (maxFetchBytes > 0 && encodeLimit - encodeOffset + networkRequestPadding <= networkRequestBudget)
                        {
                            int newCorrelationId = nextRequestId++;

                            requestRW.wrap(encodeBuffer, request.offset(), request.limit())
                                     .size(encodeLimit - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY)
                                     .apiKey(FETCH_API_KEY)
                                     .apiVersion(FETCH_API_VERSION)
                                     .correlationId(newCorrelationId)
                                     .clientId((String) null)
                                     .build();

                            OctetsFW payload = payloadRW.wrap(encodeBuffer, encodeOffset, encodeLimit)
                                    .set((b, o, m) -> m - o)
                                    .build();

                            doData(networkTarget, networkId, payload);
                            networkRequestBudget -= payload.sizeof() + networkRequestPadding;
                        }
                    }
                }
            }

            private void doBeginIfNotConnected()
            {
                if (networkId == 0L)
                {
                    // TODO: progressive back-off before reconnect
                    //       if choose to give up, say after maximum retry attempts,
                    //       then send END to each consumer to clean up

                    final long newNetworkId = supplyStreamId.getAsLong();
                    final long newCorrelationId = supplyCorrelationId.getAsLong();

                    correlations.put(newCorrelationId, NetworkConnection.this);

                    doBegin(networkTarget, newNetworkId, networkRef, newCorrelationId);
                    router.setThrottle(networkName, newNetworkId, this::handleThrottle);

                    this.networkId = newNetworkId;
                }
            }

            private void handleThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleReset(reset);
                    break;
                default:
                    // ignore
                    break;
                }
            }

            private void handleWindow(
                final WindowFW window)
            {
                final int networkWindowCredit = window.credit();
                final int networkWindowPadding = window.padding();

                this.networkRequestBudget += networkWindowCredit;
                this.networkRequestPadding = networkWindowPadding;

                doFetchIfNotInFlight();
            }

            private void handleReset(
                ResetFW reset)
            {
                doReinitialize();
                doFetchIfNotInFlight();
            }

            private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                streamState.accept(msgTypeId, buffer, index, length);
            }

            private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                if (msgTypeId == BeginFW.TYPE_ID)
                {
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    handleBegin(begin);
                }
                else
                {
                    doReset(networkReplyThrottle, networkReplyId);
                }
            }

            private void afterBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    handleData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    handleEnd(end);
                    break;
                case AbortFW.TYPE_ID:
                    final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                    handleAbort(abort);
                    break;
                default:
                    doReset(networkReplyThrottle, networkReplyId);
                    break;
                }
            }

            private void handleBegin(
                BeginFW begin)
            {
                doOfferResponseBudget();

                this.streamState = this::afterBegin;
            }

            private void handleData(
                DataFW data)
            {
                final OctetsFW payload = data.payload();

                networkResponseBudget -= payload.sizeof() + networkResponsePadding;

                if (networkResponseBudget < 0)
                {
                    doReset(networkReplyThrottle, networkReplyId);
                }
                else
                {
                    try
                    {
                        DirectBuffer networkBuffer = payload.buffer();
                        int networkOffset = payload.offset();
                        int networkLimit = payload.limit();

                        if (networkSlot != NO_SLOT)
                        {
                            final MutableDirectBuffer bufferSlot = bufferPool.buffer(networkSlot);
                            final int networkRemaining = networkLimit - networkOffset;

                            bufferSlot.putBytes(networkSlotOffset, networkBuffer, networkOffset, networkRemaining);
                            networkSlotOffset += networkRemaining;

                            networkBuffer = bufferSlot;
                            networkOffset = 0;
                            networkLimit = networkSlotOffset;
                        }

                        ResponseHeaderFW response = null;
                        if (networkOffset + BitUtil.SIZE_OF_INT + BitUtil.SIZE_OF_INT <= networkLimit)
                        {
                            response = responseRO.wrap(networkBuffer, networkOffset, networkLimit);
                            networkOffset = response.limit();
                        }

                        if (response == null || networkOffset + response.size() > networkLimit)
                        {
                            if (networkSlot == NO_SLOT)
                            {
                                networkSlot = bufferPool.acquire(networkReplyId);

                                final MutableDirectBuffer bufferSlot = bufferPool.buffer(networkSlot);
                                bufferSlot.putBytes(networkSlotOffset, payload.buffer(), payload.offset(), payload.sizeof());
                                networkSlotOffset += payload.sizeof();
                            }
                        }
                        else
                        {
                            handleFetchResponse(networkBuffer, networkOffset, networkLimit);
                            networkOffset += response.size();
                            nextResponseId++;

                            if (networkOffset < networkLimit)
                            {
                                if (networkSlot == NO_SLOT)
                                {
                                    networkSlot = bufferPool.acquire(networkReplyId);
                                }

                                final MutableDirectBuffer bufferSlot = bufferPool.buffer(networkSlot);
                                bufferSlot.putBytes(0, networkBuffer, networkOffset, networkLimit - networkOffset);
                                networkSlotOffset = networkLimit - networkOffset;
                            }
                            else
                            {
                                assert networkOffset == networkLimit;
                                networkSlotOffset = 0;
                            }

                            doOfferResponseBudget();
                            doFetchIfNotInFlight();
                        }
                    }
                    finally
                    {
                        if (networkSlotOffset == 0 && networkSlot != NO_SLOT)
                        {
                            bufferPool.release(networkSlot);
                            networkSlot = NO_SLOT;
                        }
                    }
                }
            }

            private void handleFetchResponse(
                DirectBuffer networkBuffer,
                int networkOffset,
                int networkLimit)
            {
                final FetchResponseFW fetchResponse = fetchResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
                final int topicCount = fetchResponse.topicCount();

                networkOffset = fetchResponse.limit();
                for (int topicIndex = 0; topicIndex < topicCount; topicIndex++)
                {
                    final TopicResponseFW topicResponse = topicResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
                    final String topicName = topicResponse.name().asString();
                    final int partitionCount = topicResponse.partitionCount();

                    // BUG: generated limit() ignores fixed width fields after dynamic width fields
                    networkOffset = topicResponse.limit() + BitUtil.SIZE_OF_INT;

                    final NetworkTopic topic = topicsByName.get(topicName);
                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
                    {
                        final PartitionResponseFW partitionResponse =
                                partitionResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
                        networkOffset = partitionResponse.limit();

                        final RecordSetFW recordSet = recordSetRO.wrap(networkBuffer, networkOffset, networkLimit);
                        final int recordBatchSize = recordSet.recordBatchSize();
                        networkOffset = recordSet.limit() + recordBatchSize;

                        if (topic != null)
                        {
                            topic.onPartitionResponse(partitionResponse.buffer(), partitionResponse.offset(), networkOffset);
                        }
                    }
                }
            }

            private void handleEnd(
                EndFW end)
            {
                doReinitialize();
                doFetchIfNotInFlight();
            }

            private void handleAbort(
                AbortFW abort)
            {
                doReinitialize();
                doFetchIfNotInFlight();
            }

            private void doReinitialize()
            {
                networkRequestBudget = 0;
                networkRequestPadding = 0;
                networkId = 0L;
                nextRequestId = 0;
                nextResponseId = 0;
            }

            private void doOfferResponseBudget()
            {
                final int networkResponseCredit =
                        Math.max(bufferPool.slotCapacity() - networkSlotOffset - networkResponseBudget, 0);

                doWindow(networkReplyThrottle, networkReplyId, networkResponseCredit, networkResponsePadding);

                this.networkResponseBudget += networkResponseCredit;
            }
        }
    }
}
