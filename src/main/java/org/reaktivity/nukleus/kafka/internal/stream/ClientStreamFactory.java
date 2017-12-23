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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
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
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.Varint64FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordBatchFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ClientStreamFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    final BeginFW beginRO = new BeginFW();
    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    final AbortFW abortRO = new AbortFW();

    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();
    private final KafkaBeginExFW beginExRO = new KafkaBeginExFW();
    private final OctetsFW octetsRO = new OctetsFW();

    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final KafkaDataExFW.Builder dataExRW = new KafkaDataExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final HeaderFW headerRO = new HeaderFW();

    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();
    private final RecordBatchFW recordBatchRO = new RecordBatchFW();
    private final RecordFW recordRO = new RecordFW();

    final RouteManager router;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyCorrelationId;
    final BufferPool bufferPool;
    private final MutableDirectBuffer writeBuffer;

    final Long2ObjectHashMap<NetworkConnectionPool.AbstractNetworkConnection> correlations;
    private final Map<String, Long2ObjectHashMap<NetworkConnectionPool>> connectionPools;

    public ClientStreamFactory(
        KafkaConfiguration configuration,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<NetworkConnectionPool.AbstractNetworkConnection> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = supplyCorrelationId;
        this.correlations = requireNonNull(correlations);
        this.connectionPools = new LinkedHashMap<String, Long2ObjectHashMap<NetworkConnectionPool>>();
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
                    new NetworkConnectionPool(this, networkName, ref, bufferPool));

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
            final NetworkConnectionPool.AbstractNetworkConnection connection = correlations.remove(networkReplyCorrelationId);
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
            Predicate<String> topicMatch = s -> true;
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

    void doBegin(
        final MessageConsumer target,
        final long targetId,
        final long targetRef,
        final long correlationId,
        final Flyweight.Builder.Visitor visitor)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .source("kafka")
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(b -> b.set(visitor))
                .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doData(
        final MessageConsumer target,
        final long targetId,
        final OctetsFW payload)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .groupId(0)
                .padding(0)
                .payload(p -> p.set(payload.buffer(), payload.offset(), payload.sizeof()))
                .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    void doEnd(
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

    void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int credit,
        final int padding,
        final long groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
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
        final byte[] extension)
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
        final Long2LongHashMap fetchOffsets)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .groupId(0)
                .padding(0)
                .payload(p -> p.set(payload.buffer(), payload.offset(), payload.sizeof()))
                .extension(e -> e.set(visitKafkaDataEx(fetchOffsets)))
                .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private Flyweight.Builder.Visitor visitKafkaDataEx(
        Long2LongHashMap fetchOffsets)
    {
        return (b, o, l) ->
        {
            return dataExRW.wrap(b, o, l)
                           .fetchOffsets(a ->
                           {
                               if (fetchOffsets.size() == 1)
                               {
                                   final long offset = fetchOffsets.values().iterator().nextValue();
                                   a.item(p -> p.set(offset));
                               }
                               else
                               {
                                   int partition = -1;
                                   while (fetchOffsets.get(++partition) != fetchOffsets.missingValue())
                                   {
                                       final long offset = fetchOffsets.get(partition);
                                       a.item(p -> p.set(offset));
                                   }
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

        private String applicationName;
        private long applicationCorrelationId;
        private byte[] applicationBeginExtension;
        private MessageConsumer applicationReply;
        private long applicationReplyId;
        private int applicationReplyBudget;
        private int applicationReplyPadding;

        private OctetsFW fetchKey;
        private ListFW<KafkaHeaderFW> headers;
        private int networkAttachId;

        private MessageConsumer streamState;
        private int writeableBytesMinimum;
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
            applicationName = begin.source().asString();
            applicationCorrelationId = begin.correlationId();
            final OctetsFW extension = begin.extension();

            if (extension.sizeof() == 0)
            {
                doReset(applicationThrottle, applicationId);
            }
            else
            {
                applicationBeginExtension = new byte[extension.sizeof()];
                extension.buffer().getBytes(extension.offset(), applicationBeginExtension);

                final KafkaBeginExFW beginEx = extension.get(beginExRO::wrap);

                final String topicName = beginEx.topicName().asString();
                final ArrayFW<Varint64FW> fetchOffsets = beginEx.fetchOffsets();

                this.fetchOffsets.clear();

                fetchOffsets.forEach(v -> this.fetchOffsets.put(this.fetchOffsets.size(), v.value()));

                final OctetsFW fetchKey = beginEx.fetchKey();
                byte hashCodesCount = beginEx.fetchKeyHashCount();
                if ((fetchKey != null && this.fetchOffsets.size() > 1) ||
                    (hashCodesCount > 1) ||
                    (hashCodesCount == 1 && fetchKey == null)
                   )
                {
                    doReset(applicationThrottle, applicationId);
                }
                PartitionResponseConsumer responseConsumer = this::onPartitionResponse;
                final ListFW<KafkaHeaderFW> headers = beginEx.headers();
                if (headers != null && headers.sizeof() > 0)
                {
                    MutableDirectBuffer buffer = new UnsafeBuffer(new byte[headers.sizeof()]);
                    buffer.putBytes(0, headers.buffer(), headers.offset(), headers.sizeof());
                    this.headers = new ListFW<KafkaHeaderFW>(new KafkaHeaderFW()).wrap(buffer, 0, buffer.capacity());
                    responseConsumer = this::onPartitionResponseFiltered;
                }
                if (fetchKey != null)
                {
                    MutableDirectBuffer buffer = new UnsafeBuffer(new byte[fetchKey.sizeof()]);
                    buffer.putBytes(0, fetchKey.buffer(), fetchKey.offset(), fetchKey.sizeof());
                    this.fetchKey = new OctetsFW().wrap(buffer, 0, buffer.capacity());
                    int hashCode = hashCodesCount == 1 ? beginEx.fetchKeyHash().nextInt()
                            : BufferUtil.defaultHashCode(fetchKey.buffer(), fetchKey.offset(), fetchKey.limit());
                    networkPool.doAttach(topicName, this.fetchOffsets, hashCode, this::onPartitionResponseFiltered,
                            this::writeableBytes, this::onAttached, this::onMetadataError);
                }
                else
                {
                    networkPool.doAttach(topicName, this.fetchOffsets, responseConsumer,
                            this::writeableBytes, this::onAttached, this::onMetadataError);
                }
                this.streamState = this::afterBegin;
            }
        }

        private void onAttached(int newNetworkAttachId)
        {
            final long newReplyId = supplyStreamId.getAsLong();
            final String replyName = applicationName;
            final MessageConsumer newReply = router.supplyTarget(replyName);

            doKafkaBegin(newReply, newReplyId, 0L, applicationCorrelationId, applicationBeginExtension);
            router.setThrottle(applicationName, newReplyId, this::handleThrottle);

            doWindow(applicationThrottle, applicationId, 0, 0, 0);

            this.networkAttachId = newNetworkAttachId;
            this.applicationReply = newReply;
            this.applicationReplyId = newReplyId;
        }

        private void onMetadataError(int errorCode)
        {
            doReset(applicationThrottle, applicationId);
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

        private boolean headerMatches(
            final HeaderFW header)
        {
            return !headers.anyMatch(kh ->
            {
                boolean keyMatches = BufferUtil.matches(header.key(), kh.key());
                boolean valueMatches = BufferUtil.matches(kh.value(), header.value());
                return keyMatches && !valueMatches;
            });
        }

        private void onPartitionResponse(
            DirectBuffer buffer,
            int offset,
            int length,
            long requestedOffset,
            PartitionProgressHandler progressHandler)
        {
            final int maxLimit = offset + length;

            int networkOffset = offset;

            final PartitionResponseFW partition = partitionResponseRO.wrap(buffer, networkOffset, maxLimit);
            networkOffset = partition.limit();

            final int partitionId = partition.partitionId();
            final long firstFetchAt = fetchOffsets.get(partitionId);

            long nextFetchAt = firstFetchAt;

            // TODO: determine appropriate reaction to different non-zero error codes
            if (partition.errorCode() == 0 && networkOffset < maxLimit - BitUtil.SIZE_OF_INT
                    && requestedOffset <= firstFetchAt) // avoid out of order delivery
            {
                final RecordSetFW recordSet = recordSetRO.wrap(buffer, networkOffset, maxLimit);
                networkOffset = recordSet.limit();

                final int recordSetLimit = networkOffset + recordSet.recordBatchSize();
                if (recordSetLimit <= maxLimit)
                {
                    loop:
                    while (networkOffset < recordSetLimit - RecordBatchFW.FIELD_OFFSET_RECORD_COUNT - BitUtil.SIZE_OF_INT)
                    {
                        final RecordBatchFW recordBatch = recordBatchRO.wrap(buffer, networkOffset, recordSetLimit);
                        networkOffset = recordBatch.limit();

                        final int recordBatchLimit = recordBatch.offset() +
                                RecordBatchFW.FIELD_OFFSET_LENGTH + BitUtil.SIZE_OF_INT + recordBatch.length();
                        if (recordBatchLimit > recordSetLimit)
                        {
                            break loop;
                        }

                        final long firstOffset = recordBatch.firstOffset();

                        while (networkOffset < recordBatchLimit - 7 /* minimum RecordFW size */)
                        {
                            final RecordFW record = recordRO.wrap(buffer, networkOffset, recordBatchLimit);
                            networkOffset = record.limit();

                            final int recordLimit = record.offset() +
                                    RecordFW.FIELD_OFFSET_ATTRIBUTES + record.length();
                            if (recordLimit > recordSetLimit)
                            {
                                break loop;
                            }

                            final int headerCount = record.headerCount();

                            for (int headerIndex = 0; headerIndex < headerCount; headerIndex++)
                            {
                                final HeaderFW headerRO = new HeaderFW();
                                final HeaderFW header = headerRO.wrap(buffer, networkOffset, recordBatchLimit);
                                networkOffset = header.limit();
                            }

                            final long currentFetchAt = firstOffset + record.offsetDelta();

                            if (currentFetchAt >= firstFetchAt)
                            {
                                final OctetsFW value = record.value();

                                if (applicationReplyBudget < value.sizeof() + applicationReplyPadding)
                                {
                                    writeableBytesMinimum = value.sizeof() + applicationReplyPadding;
                                    break loop;
                                }

                                nextFetchAt = currentFetchAt + 1;

                                // assumes partitions "discovered" is numerical order
                                // currently guaranteed only for single partition topics
                                this.fetchOffsets.put(partitionId, nextFetchAt);

                                doKafkaData(applicationReply, applicationReplyId, value, fetchOffsets);
                                applicationReplyBudget -= value.sizeof() + applicationReplyPadding;
                                writeableBytesMinimum = 0;
                            }
                        }
                    }

                    if (nextFetchAt > firstFetchAt)
                    {
                        progressHandler.handle(partitionId, firstFetchAt, nextFetchAt);
                    }
                }
            }
        }

        private void onPartitionResponseFiltered(
            DirectBuffer buffer,
            int offset,
            int length,
            long requestedOffset,
            PartitionProgressHandler progressHandler)
        {
            final int maxLimit = offset + length;

            int networkOffset = offset;

            final PartitionResponseFW partition = partitionResponseRO.wrap(buffer, networkOffset, maxLimit);
            networkOffset = partition.limit();

            final int partitionId = partition.partitionId();
            final long firstFetchAt = fetchOffsets.get(partitionId);

            long nextFetchAt = firstFetchAt;

            // TODO: determine appropriate reaction to different non-zero error codes
            if (partition.errorCode() == 0 && networkOffset < maxLimit - BitUtil.SIZE_OF_INT
                    && requestedOffset <= firstFetchAt) // avoid out of order delivery
            {
                final RecordSetFW recordSet = recordSetRO.wrap(buffer, networkOffset, maxLimit);
                networkOffset = recordSet.limit();

                final int recordSetLimit = networkOffset + recordSet.recordBatchSize();
                if (recordSetLimit <= maxLimit)
                {
                    loop:
                    while (networkOffset < recordSetLimit - RecordBatchFW.FIELD_OFFSET_RECORD_COUNT - BitUtil.SIZE_OF_INT)
                    {
                        final RecordBatchFW recordBatch = recordBatchRO.wrap(buffer, networkOffset, recordSetLimit);
                        networkOffset = recordBatch.limit();

                        final int recordBatchLimit = recordBatch.offset() +
                                RecordBatchFW.FIELD_OFFSET_LENGTH + BitUtil.SIZE_OF_INT + recordBatch.length();
                        if (recordBatchLimit > recordSetLimit)
                        {
                            break loop;
                        }

                        final long firstOffset = recordBatch.firstOffset();
                        OctetsFW lastMatchingValue = null;
                        long lastMatchingOffset = firstOffset;

                        while (networkOffset < recordBatchLimit - 7 /* minimum RecordFW size */)
                        {
                            final RecordFW record = recordRO.wrap(buffer, networkOffset, recordBatchLimit);
                            networkOffset = record.limit();

                            final int recordLimit = record.offset() +
                                    RecordFW.FIELD_OFFSET_ATTRIBUTES + record.length();
                            if (recordLimit > recordSetLimit)
                            {
                                break loop;
                            }

                            final OctetsFW key = record.key();
                            boolean matches = this.fetchKey == null || BufferUtil.matches(key, this.fetchKey);

                            final int headerCount = record.headerCount();

                            for (int headerIndex = 0; headerIndex < headerCount; headerIndex++)
                            {
                                final HeaderFW header = headerRO.wrap(buffer, networkOffset, recordBatchLimit);
                                matches = matches && headerMatches(header);
                                networkOffset = header.limit();
                            }

                            final long currentFetchAt = firstOffset + record.offsetDelta();
                            if (currentFetchAt >= firstFetchAt)
                            {
                                if (matches)
                                {
                                    // Send the previous matching message now we know what the new offset should be
                                    // (including any nonmatching skipped messages)
                                    if (lastMatchingValue != null)
                                    {
                                        if (applicationReplyBudget < lastMatchingValue.sizeof() + applicationReplyPadding)
                                        {
                                            writeableBytesMinimum = lastMatchingValue.sizeof() + applicationReplyPadding;
                                            nextFetchAt = lastMatchingOffset;
                                            this.fetchOffsets.put(partitionId, nextFetchAt);
                                            break loop;
                                        }
                                        this.fetchOffsets.put(partitionId, currentFetchAt);
                                        doKafkaData(applicationReply, applicationReplyId, lastMatchingValue, fetchOffsets);
                                        applicationReplyBudget -= lastMatchingValue.sizeof() + applicationReplyPadding;
                                        writeableBytesMinimum = 0;
                                    }
                                    final OctetsFW value = record.value();
                                    lastMatchingValue = octetsRO.wrap(buffer, value.offset(), value.limit());
                                    lastMatchingOffset = currentFetchAt;
                                }
                                nextFetchAt = currentFetchAt + 1;
                                this.fetchOffsets.put(partitionId, nextFetchAt);
                            }
                        }
                        if (lastMatchingValue != null)
                        {
                            if (applicationReplyBudget < lastMatchingValue.sizeof() + applicationReplyPadding)
                            {
                                writeableBytesMinimum = lastMatchingValue.sizeof() + applicationReplyPadding;
                                nextFetchAt = lastMatchingOffset;
                                this.fetchOffsets.put(partitionId, nextFetchAt);
                                break loop;
                            }
                            nextFetchAt = recordBatch.firstOffset() + recordBatch.lastOffsetDelta() + 1;
                            this.fetchOffsets.put(partitionId, nextFetchAt);
                            doKafkaData(applicationReply, applicationReplyId, lastMatchingValue, fetchOffsets);
                            applicationReplyBudget -= lastMatchingValue.sizeof() + applicationReplyPadding;
                            writeableBytesMinimum = 0;
                        }
                    } // end loop

                    if (nextFetchAt > firstFetchAt)
                    {
                        progressHandler.handle(partitionId, firstFetchAt, nextFetchAt);
                    }
                }
            }
        }

        private int writeableBytes()
        {
            final int writeableBytes = applicationReplyBudget - applicationReplyPadding;
            return writeableBytes > writeableBytesMinimum ? writeableBytes : 0;
        }
    }
}
