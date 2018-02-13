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
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.isFin;
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.isReset;
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.RST;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;
import org.reaktivity.nukleus.buffer.MemoryManager;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.AckFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.RegionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.TransferFW;
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ClientStreamFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    final BeginFW beginRO = new BeginFW();
    final TransferFW transferRO = new TransferFW();
    final RegionFW regionRO = new RegionFW();
    final AckFW ackRO = new AckFW();

    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();
    private final KafkaBeginExFW beginExRO = new KafkaBeginExFW();
    private final OctetsFW keyRO = new OctetsFW();
    private final OctetsFW valueRO = new OctetsFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final TransferFW.Builder transferRW = new TransferFW.Builder();
    private final RegionFW.Builder regionRW = new RegionFW.Builder();
    private final AckFW.Builder ackRW = new AckFW.Builder();

    private final KafkaDataExFW.Builder dataExRW = new KafkaDataExFW.Builder();

    private final HeaderFW headerRO = new HeaderFW();

    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();
    private final RecordBatchFW recordBatchRO = new RecordBatchFW();
    private final RecordFW recordRO = new RecordFW();

    final RouteManager router;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyCorrelationId;
    final MemoryManager memoryManager;
    final Supplier<DirectBufferBuilder> supplyDirectBufferBuilder;
    private final MutableDirectBuffer writeBuffer;

    final Long2ObjectHashMap<NetworkConnectionPool.AbstractNetworkConnection> correlations;
    private final Map<String, Long2ObjectHashMap<NetworkConnectionPool>> connectionPools;

    public ClientStreamFactory(
        KafkaConfiguration configuration,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        MemoryManager memoryManager,
        Supplier<DirectBufferBuilder> supplyDirectBufferBuilder,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<NetworkConnectionPool.AbstractNetworkConnection> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.memoryManager = requireNonNull(memoryManager);
        this.supplyDirectBufferBuilder = supplyDirectBufferBuilder;
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
                    new NetworkConnectionPool(this, networkName, ref, memoryManager));

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

    void doTransfer(
        final MessageConsumer target,
        final long targetId,
        final int flags,
        ListFW<RegionFW> regions)
    {
        final TransferFW transfer = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .flags(flags)
                .regions(m -> regions.forEach(r -> m.item(i -> i.address(r.address()).length(r.length()))))
                .build();

        target.accept(transfer.typeId(), transfer.buffer(), transfer.offset(), transfer.sizeof());
    }

    public void doTransfer(
        final MessageConsumer target,
        final long targetId,
        final int flags)
    {
        final TransferFW transfer = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .flags(flags)
                .build();

        target.accept(transfer.typeId(), transfer.buffer(), transfer.offset(), transfer.sizeof());
    }

    public void doAck(
        final MessageConsumer throttle,
        final long throttleId,
        final int flags,
        final ListFW<RegionFW> regions)
    {
        final AckFW ack = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .flags(flags)
                .regions(m -> regions.forEach(r -> m.item(i -> i.address(r.address()).length(r.length()))))
                .build();

        throttle.accept(ack.typeId(), ack.buffer(), ack.offset(), ack.sizeof());
    }

    public void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final AckFW ack = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .flags(0x02) // rst
                .build();

        throttle.accept(ack.typeId(), ack.buffer(), ack.offset(), ack.sizeof());
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
        final int flags,
        ListFW<RegionFW> regions,
        final Long2LongHashMap fetchOffsets,
        final OctetsFW messageKey)
    {
        final TransferFW data = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .flags(flags)
                .regions(m -> regions.forEach(r -> m.item(i -> i.address(r.address()).length(r.length()))))
                .extension(e -> e.set(visitKafkaDataEx(fetchOffsets, messageKey)))
                .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private Flyweight.Builder.Visitor visitKafkaDataEx(
        Long2LongHashMap fetchOffsets,
        final OctetsFW messageKey)
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
                    .messageKey(messageKey)
                    .build()
                    .sizeof();
        };
    }

    private final class ClientAcceptStream
    {
        private static final int UNATTACHED = -1;

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
        private int networkAttachId = UNATTACHED;

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
            case TransferFW.TYPE_ID:
                int flags = transferRO.wrap(buffer, index, length).flags();
                if (isFin(flags))
                {
                    // accept reply stream is allowed to outlive accept stream, so ignore END
                }
                else if (isReset(flags))
                {
                    doTransfer(applicationReply, applicationReplyId, RST);
                    networkPool.doDetach(networkAttachId, fetchOffsets);
                    networkAttachId = UNATTACHED;
                }
                else
                {
                    doReset(applicationThrottle, applicationId);
                    networkPool.doDetach(networkAttachId, fetchOffsets);
                    networkAttachId = UNATTACHED;
                }
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
                if (headers != null && !headers.isEmpty())
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
            case AckFW.TYPE_ID:
                final AckFW ack = ackRO.wrap(buffer, index, index + length);
                handleAck(ack);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleAck(
            final AckFW ack)
        {
            if ((ack.flags() & RST) == RST)
            {
                handleReset();
            }
            // TODO: pass the ACK on to NetworkConnectionPool
            networkPool.doFlush(networkAttachId);
        }

        private void handleReset()
        {
            networkPool.doDetach(networkAttachId, fetchOffsets);
            networkAttachId = UNATTACHED;
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
            boolean isCompactedTopic,
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
            if (networkAttachId != UNATTACHED && partition.errorCode() == 0 && networkOffset < maxLimit - BitUtil.SIZE_OF_INT
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
                                final int payloadLength = value == null ? 0 : value.sizeof();

                                if (applicationReplyBudget < payloadLength + applicationReplyPadding)
                                {
                                    writeableBytesMinimum = payloadLength + applicationReplyPadding;
                                    break loop;
                                }

                                nextFetchAt = currentFetchAt + 1;

                                // assumes partitions "discovered" is numerical order
                                // currently guaranteed only for single partition topics
                                this.fetchOffsets.put(partitionId, nextFetchAt);

                                final OctetsFW messageKey = isCompactedTopic ? record.key(): null;

                                doKafkaData(applicationReply, applicationReplyId, applicationReplyPadding, value,
                                        fetchOffsets, messageKey);
                                applicationReplyBudget -= payloadLength + applicationReplyPadding;
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
            boolean isCompactedTopic,
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
            if (networkAttachId != UNATTACHED && partition.errorCode() == 0 && networkOffset < maxLimit - BitUtil.SIZE_OF_INT
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
                        OctetsFW lastMatchingKey = null;
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
                                        final OctetsFW messageKey = isCompactedTopic ? lastMatchingKey : null;
                                        doKafkaData(applicationReply, applicationReplyId, applicationReplyPadding,
                                                    lastMatchingValue, fetchOffsets, messageKey);
                                        applicationReplyBudget -= lastMatchingValue.sizeof() + applicationReplyPadding;
                                        writeableBytesMinimum = 0;
                                    }
                                    final OctetsFW value = record.value();
                                    lastMatchingValue = valueRO.wrap(buffer, value.offset(), value.limit());
                                    lastMatchingKey = key == null ? null : keyRO.wrap(buffer, key.offset(), key.limit());
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
                            final OctetsFW messageKey = isCompactedTopic ? lastMatchingKey : null;
                            doKafkaData(applicationReply, applicationReplyId, applicationReplyPadding,
                                        lastMatchingValue, fetchOffsets, messageKey);
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
