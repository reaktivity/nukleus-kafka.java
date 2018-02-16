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
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.RST;
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.isFin;
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.isReset;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

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
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.Varint64FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.region.RegionMetadataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AckFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaTransferExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.RegionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.TransferFW;
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;
import org.reaktivity.nukleus.kafka.internal.util.FrameFlags;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ClientStreamFactory implements StreamFactory
{
    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    static final int REGION_METADATA_SIZE;

    static
    {
        MutableDirectBuffer scratch = new UnsafeBuffer(new byte[100]);
        REGION_METADATA_SIZE = new RegionMetadataFW.Builder()
                .wrap(scratch, 0, scratch.capacity())
                .endAdress(0L)
                .regionCapacity(0)
                .refCount(1)
                .build()
                .limit();
    }

    private final RouteFW routeRO = new RouteFW();
    final BeginFW beginRO = new BeginFW();
    final TransferFW transferRO = new TransferFW();
    final RegionFW regionRO = new RegionFW();

    final UnsafeBuffer regionMetadataBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    final RegionMetadataFW.Builder regionMetadataRW = new RegionMetadataFW.Builder();
    final RegionMetadataFW regionMetadataRO = new RegionMetadataFW();
    final AckFW ackRO = new AckFW();

    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();
    private final KafkaBeginExFW beginExRO = new KafkaBeginExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final TransferFW.Builder transferRW = new TransferFW.Builder();
    private final AckFW.Builder ackRW = new AckFW.Builder();

    private final KafkaTransferExFW.Builder transferExRW = new KafkaTransferExFW.Builder();

    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();
    final OctetsFW octetsRO = new OctetsFW();

    final RouteManager router;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyCorrelationId;
    final MemoryManager memoryManager;
    final DirectBufferBuilder directBufferBuilder;
    final KafkaConfiguration configuration;
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
        this.directBufferBuilder = supplyDirectBufferBuilder.get();
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = supplyCorrelationId;
        this.correlations = requireNonNull(correlations);
        this.connectionPools = new LinkedHashMap<String, Long2ObjectHashMap<NetworkConnectionPool>>();
        this.configuration = configuration;
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
        Consumer<ListFW.Builder<RegionFW.Builder, RegionFW>> mutator)
    {
        final TransferFW transfer = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .flags(flags)
                .regions(mutator)
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
                .regions(regions == null ? m ->
                         {} : m -> regions.forEach(r -> m.item(i -> i.address(r.address()).length(r.length()))))
                .build();

        throttle.accept(ack.typeId(), ack.buffer(), ack.offset(), ack.sizeof());
    }

    public void doAckFin(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final AckFW ack = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .flags(FrameFlags.FIN)
                .build();

        throttle.accept(ack.typeId(), ack.buffer(), ack.offset(), ack.sizeof());
    }

    public void doAckReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final AckFW ack = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .flags(FrameFlags.RST)
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

    private void doKafkaTransfer(
        final MessageConsumer target,
        final long targetId,
        final int flags,
        final DirectBuffer value,
        final Long2LongHashMap fetchOffsets,
        final DirectBuffer messageKey)
    {
        long regionAddress = value == null ? 0 : value.addressOffset() - memoryManager.resolve(0);
        OctetsFW octetsKey = messageKey == null ? null : octetsRO.wrap(messageKey, 0,  messageKey.capacity());
        final TransferFW transfer = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .flags(flags)
                .regions(m ->
                {
                    if (value != null)
                    {
                        m.item(i -> i.address(regionAddress).length(value.capacity()).streamId(targetId));
                    }
                })
                .extension(e -> e.set(visitKafkaDataEx(fetchOffsets, octetsKey)))
                .build();

        target.accept(transfer.typeId(), transfer.buffer(), transfer.offset(), transfer.sizeof());
    }

    private Flyweight.Builder.Visitor visitKafkaDataEx(
        Long2LongHashMap fetchOffsets,
        final OctetsFW messageKey)
    {
        return (b, o, l) ->
        {
            return transferExRW.wrap(b, o, l)
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

    private final class ClientAcceptStream implements MessageDispatcher
    {
        private static final int UNATTACHED = -1;

        private final MessageConsumer applicationThrottle;
        private final long applicationId;
        private final NetworkConnectionPool networkPool;
        private final Long2LongHashMap fetchOffsets;
        private final UnsafeBuffer pendingMessageValueBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
        private final UnsafeBuffer pendingMessageKeyBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

        private String applicationName;
        private long applicationCorrelationId;
        private byte[] applicationBeginExtension;
        private MessageConsumer applicationReply;
        private long applicationReplyId;

        private int networkAttachId = UNATTACHED;
        private boolean compacted;
        private PartitionProgressHandler progressHandler;

        private MessageConsumer streamState;

        private boolean messagePending;;
        private UnsafeBuffer pendingMessageValue;
        private UnsafeBuffer pendingMessageKey;

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
                doAckReset(applicationThrottle, applicationId);
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
                int flags = transferRO.wrap(buffer, index, index + length).flags();
                if (isFin(flags))
                {
                    // accept reply stream is allowed to outlive accept stream, so do clean close
                    doAckFin(applicationThrottle, applicationId);
                }
                else if (isReset(flags))
                {
                    doTransfer(applicationReply, applicationReplyId, RST);
                    networkPool.doDetach(networkAttachId, fetchOffsets);
                    networkAttachId = UNATTACHED;
                }
                else
                {
                    doAckReset(applicationThrottle, applicationId);
                    networkPool.doDetach(networkAttachId, fetchOffsets);
                    networkAttachId = UNATTACHED;
                }
                break;
            default:
                doAckReset(applicationThrottle, applicationId);
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
                doAckReset(applicationThrottle, applicationId);
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
                    doAckReset(applicationThrottle, applicationId);
                }
                else
                {
                    final ListFW<KafkaHeaderFW> headers = beginEx.headers();
                    int hashCode = -1;
                    if (fetchKey != null)
                    {
                        hashCode = hashCodesCount == 1 ? beginEx.fetchKeyHash().nextInt()
                                : BufferUtil.defaultHashCode(fetchKey.buffer(), fetchKey.offset(), fetchKey.limit());
                    }
                    networkPool.doAttach(topicName, this.fetchOffsets, hashCode, fetchKey, headers,
                            this, h -> progressHandler = h, this::onAttached, this::onMetadataError);
                    this.streamState = this::afterBegin;
                }
            }
        }

        private void onAttached(int newNetworkAttachId, boolean compacted)
        {
            final long newReplyId = supplyStreamId.getAsLong();
            final String replyName = applicationName;
            final MessageConsumer newReply = router.supplyTarget(replyName);

            doKafkaBegin(newReply, newReplyId, 0L, applicationCorrelationId, applicationBeginExtension);
            router.setThrottle(applicationName, newReplyId, this::handleThrottle);

            this.networkAttachId = newNetworkAttachId;
            this.compacted = compacted;
            this.applicationReply = newReply;
            this.applicationReplyId = newReplyId;
        }

        private void onMetadataError(int errorCode)
        {
            doAckReset(applicationThrottle, applicationId);
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
            ack.regions().forEach(r -> handleAcknowledgedRegion(r));
            if ((ack.flags() & RST) == RST)
            {
                handleReset();
            }
        }

        private void handleAcknowledgedRegion(
            final RegionFW region)
        {
            // Free the region if refCount (in region metadata) is now 0 and this is the last ack
            // for the region (partial acks case: we assume in order acks)
            final long ackEndAddress = region.address() + region.length();
            regionMetadataBuffer.wrap(memoryManager.resolve(ackEndAddress),
                    REGION_METADATA_SIZE);
            regionMetadataRO.wrap(regionMetadataBuffer,  0,  regionMetadataBuffer.capacity());
            long endAddress = regionMetadataRO.endAdress();
            if (endAddress == ackEndAddress) // region is now fully ack'd
            {
                int regionCapacity = regionMetadataRO.regionCapacity();
                int newRefCount = regionMetadataRO.refCount() - 1;
                assert newRefCount >= 0;
                if (newRefCount == 0)
                {
                    memoryManager.release(endAddress - regionCapacity + REGION_METADATA_SIZE, regionCapacity);
                }
                else
                {
                    regionMetadataRW.wrap(regionMetadataBuffer,  0,  regionMetadataBuffer.capacity())
                        .endAdress(ackEndAddress)
                        .regionCapacity(regionCapacity)
                        .refCount(newRefCount)
                        .build();
                }
                networkPool.doFlush();
            }
        }

        private void handleReset()
        {
            networkPool.doDetach(networkAttachId, fetchOffsets);
            networkAttachId = UNATTACHED;
            doAckReset(applicationThrottle, applicationId);
        }

        @Override
        public int dispatch(int partition, long requestOffset, long messageOffset, DirectBuffer key,
                            Function<DirectBuffer, DirectBuffer> supplyHeader, DirectBuffer value)
        {
            int  messagesDelivered = 0;
            long lastOffset = fetchOffsets.get(partition);
            if (requestOffset <= lastOffset // avoid out of order deliveryYeah‰
                    && messageOffset > lastOffset)
            {
                flush(partition, requestOffset, messageOffset - 1);
                if (key == null)
                {
                    pendingMessageKey = null;
                }
                else
                {
                    pendingMessageKeyBuffer.wrap(key);
                    pendingMessageKey = pendingMessageKeyBuffer;
                }
                if (value == null)
                {
                    pendingMessageValue = null;
                }
                else
                {
                    pendingMessageValueBuffer.wrap(value);
                    pendingMessageValue = pendingMessageValueBuffer;
                }
                messagePending = true;
                messagesDelivered++;
            }
            return messagesDelivered;
        }

        @Override
        public void flush(int partition, long requestOffset, long lastOffset)
        {
            if (messagePending)
            {
                long previousOffset = fetchOffsets.get(partition);
                this.fetchOffsets.put(partition, lastOffset);
                doKafkaTransfer(applicationReply, applicationReplyId, 0,
                        pendingMessageValue, fetchOffsets, compacted ? pendingMessageKey : null);
                messagePending = false;
                progressHandler.handle(partition, previousOffset, lastOffset);
            }
        }
    }
}
