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
import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.EMPTY_BYTE_ARRAY;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.cache.DefaultMessageCache;
import org.reaktivity.nukleus.kafka.internal.cache.MessageCache;
import org.reaktivity.nukleus.kafka.internal.function.IntBooleanConsumer;
import org.reaktivity.nukleus.kafka.internal.function.PartitionProgressHandler;
import org.reaktivity.nukleus.kafka.internal.memory.MemoryManager;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.Varint64FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ClientStreamFactory implements StreamFactory
{
    static final long UNSET = -1;

    private final UnsafeBuffer workBuffer1 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final UnsafeBuffer workBuffer2 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

    private final RouteFW routeRO = new RouteFW();

    final BeginFW beginRO = new BeginFW();
    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    final AbortFW abortRO = new AbortFW();

    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();
    private final KafkaBeginExFW beginExRO = new KafkaBeginExFW();

    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final KafkaDataExFW.Builder dataExRW = new KafkaDataExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();

    private final OctetsFW messageKeyRO = new OctetsFW();
    private final OctetsFW messageValueRO = new OctetsFW();

    final RouteManager router;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyTrace;
    final LongSupplier supplyCorrelationId;
    final BufferPool bufferPool;
    final MessageCache messageCache;
    private final MutableDirectBuffer writeBuffer;

    final Long2ObjectHashMap<NetworkConnectionPool.AbstractNetworkConnection> correlations;

    private final Long2LongHashMap groupBudget;
    private final Long2LongHashMap groupMembers;

    private final Map<String, Long2ObjectHashMap<NetworkConnectionPool>> connectionPools;
    private final int fetchMaxBytes;
    private final int fetchPartitionMaxBytes;

    public ClientStreamFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        MemoryManager memoryManager,
        LongSupplier supplyStreamId,
        LongSupplier supplyTrace,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<NetworkConnectionPool.AbstractNetworkConnection> correlations,
        Map<String, Long2ObjectHashMap<NetworkConnectionPool>> connectionPools,
        Consumer<BiFunction<String, Long, NetworkConnectionPool>> connectPoolFactoryConsumer)
    {
        this.fetchMaxBytes = config.fetchMaxBytes();
        this.fetchPartitionMaxBytes = config.fetchPartitionMaxBytes();
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.messageCache = new DefaultMessageCache(requireNonNull(memoryManager));
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyTrace = requireNonNull(supplyTrace);
        this.supplyCorrelationId = supplyCorrelationId;
        this.correlations = requireNonNull(correlations);
        this.connectionPools = connectionPools;
        groupBudget = new Long2LongHashMap(-1);
        groupMembers = new Long2LongHashMap(-1);
        connectPoolFactoryConsumer.accept((networkName, ref) ->
            new NetworkConnectionPool(this, networkName, ref, fetchMaxBytes, fetchPartitionMaxBytes, bufferPool, messageCache));
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
                final OctetsFW routeExtension = route.extension();
                ListFW<KafkaHeaderFW> routeHeaders = null;
                if (routeExtension.sizeof() > 0)
                {
                    final KafkaRouteExFW routeEx = routeExtension.get(routeExRO::wrap);
                    routeHeaders = routeEx.headers();
                }

                Long2ObjectHashMap<NetworkConnectionPool> connectionPoolsByRef =
                    connectionPools.computeIfAbsent(networkName, this::newConnectionPoolsByRef);

                NetworkConnectionPool connectionPool = connectionPoolsByRef.computeIfAbsent(networkRef,
                        ref -> new NetworkConnectionPool(this, networkName, ref, fetchMaxBytes, fetchPartitionMaxBytes,
                                bufferPool, messageCache));

                newStream = new ClientAcceptStream(applicationThrottle, applicationId, connectionPool,
                        routeHeaders)::handleStream;
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
                .trace(supplyTrace.getAsLong())
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
        final int padding,
        final OctetsFW payload)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(supplyTrace.getAsLong())
                .groupId(0)
                .padding(padding)
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
                .trace(supplyTrace.getAsLong())
                .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        final MessageConsumer target,
        final long targetId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(supplyTrace.getAsLong())
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
                .trace(supplyTrace.getAsLong())
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
               .trace(supplyTrace.getAsLong())
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
                .trace(supplyTrace.getAsLong())
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
        final long traceId,
        final int padding,
        final DirectBuffer messageKey,
        final long timestamp,
        final DirectBuffer messageValue,
        final Long2LongHashMap fetchOffsets)
    {
        OctetsFW key = messageKey == null ? null : messageKeyRO.wrap(messageKey, 0, messageKey.capacity());
        OctetsFW value = messageValue == null ? null : messageValueRO.wrap(messageValue, 0, messageValue.capacity());
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(traceId)
                .groupId(0)
                .padding(padding)
                .payload(value)
                .extension(e -> e.set(visitKafkaDataEx(timestamp, fetchOffsets, key)))
                .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private Flyweight.Builder.Visitor visitKafkaDataEx(
        long timestamp,
        Long2LongHashMap fetchOffsets,
        final OctetsFW messageKey)
    {
        return (b, o, l) ->
        {
            return dataExRW.wrap(b, o, l)
                    .timestamp(timestamp)
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
        private final ListFW<KafkaHeaderFW> routeHeaders;

        private final Long2LongHashMap fetchOffsets;

        private boolean subscribedByKey;

        private String applicationName;
        private long applicationCorrelationId;
        private byte[] applicationBeginExtension;
        private MessageConsumer applicationReply;
        private long applicationReplyId;
        private int applicationReplyPadding;

        private int networkAttachId = UNATTACHED;
        private boolean compacted;
        private PartitionProgressHandler progressHandler;

        private MessageConsumer streamState;
        private int writeableBytesMinimum;

        private final DirectBuffer pendingMessageKeyBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
        private final DirectBuffer pendingMessageValueBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

        private boolean messagePending;;
        private DirectBuffer pendingMessageKey;
        private long pendingMessageTimestamp;
        private long pendingMessageTraceId;
        private DirectBuffer pendingMessageValue;

        private long progressStartOffset = UNSET;
        private long progressEndOffset;
        private boolean firstWindow = true;
        private long groupId;
        private Budget budget;

        private Consumer<IntBooleanConsumer> attacher;

        private ClientAcceptStream(
            MessageConsumer applicationThrottle,
            long applicationId,
            NetworkConnectionPool networkPool,
            ListFW<KafkaHeaderFW> routeHeaders)
        {
            this.applicationThrottle = applicationThrottle;
            this.applicationId = applicationId;
            this.networkPool = networkPool;
            this.fetchOffsets = new Long2LongHashMap(-1L);
            this.streamState = this::beforeBegin;
            this.routeHeaders = routeHeaders;
            budget = new StreamBudget();
        }

        @Override
        public int dispatch(
            int partition,
            long requestOffset,
            long messageOffset,
            DirectBuffer key,
            Function<DirectBuffer, DirectBuffer> supplyHeader,
            long timestamp,
            long traceId,
            DirectBuffer value)
        {
            int result = MessageDispatcher.FLAGS_MATCHED;
            if (progressStartOffset == UNSET)
            {
                progressStartOffset = fetchOffsets.get(partition);
                progressEndOffset = progressStartOffset;
            }
            if (compacted && (subscribedByKey || equals(key, pendingMessageKey)))
            {
                // This message replaces the last one
                if (messagePending)
                {
                    messagePending = false;
                    final int previousLength = pendingMessageValue == null ? 0 : pendingMessageValue.capacity();
                    budget.incApplicationReplyBudget(previousLength + applicationReplyPadding);
                }
                writeableBytesMinimum = 0;
            }
            else
            {
                flushPreviousMessage(partition, messageOffset - 1);
            }

            if (requestOffset <= progressStartOffset // avoid out of order delivery
                && messageOffset > progressStartOffset)
            {
                final int payloadLength = value == null ? 0 : value.capacity();
                int applicationReplyBudget = budget.applicationReplyBudget();
                if (applicationReplyBudget == 0 || applicationReplyBudget < payloadLength + applicationReplyPadding)
                {
                    writeableBytesMinimum = payloadLength + applicationReplyPadding;
                }
                else
                {
                    writeableBytesMinimum = 0;
                    budget.decApplicationReplyBudget(payloadLength + applicationReplyPadding);
                    assert budget.applicationReplyBudget() >= 0;

                    pendingMessageKey = wrap(pendingMessageKeyBuffer, key);
                    pendingMessageTimestamp = timestamp;
                    pendingMessageTraceId = traceId;
                    pendingMessageValue = wrap(pendingMessageValueBuffer, value);
                    messagePending = true;
                    result &= MessageDispatcher.FLAGS_DELIVERED;
                }
            }
            return result;
        }

        @Override
        public void flush(int partition, long requestOffset, long lastOffset)
        {
            flushPreviousMessage(partition, lastOffset);
            long startOffset = progressStartOffset;
            long endOffset = progressEndOffset;
            if (startOffset == UNSET)
            {
                // no messages were dispatched
                startOffset = fetchOffsets.get(partition);
                endOffset = lastOffset;
            }
            else if (requestOffset <= startOffset
                    && writeableBytesMinimum == 0)
            {
                // We didn't skip any messages due to lack of window, advance to highest offset
                endOffset = lastOffset;
            }
            if (endOffset > startOffset && requestOffset <= startOffset)
            {
                this.fetchOffsets.put(partition, endOffset);
                progressHandler.handle(partition, startOffset, endOffset);
            }
            progressStartOffset = UNSET;
        }

        private void flushPreviousMessage(
                int partition,
                long messageOffset)
        {
            if (messagePending)
            {
                this.fetchOffsets.put(partition, messageOffset);
                doKafkaData(applicationReply, applicationReplyId, pendingMessageTraceId, applicationReplyPadding,
                            compacted ? pendingMessageKey : null,
                            pendingMessageTimestamp, pendingMessageValue, fetchOffsets);
                messagePending = false;
                progressEndOffset = messageOffset;
            }
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
                budget.leaveGroup();
                doAbort(applicationReply, applicationReplyId);
                networkPool.doDetach(networkAttachId, fetchOffsets);
                networkAttachId = UNATTACHED;
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

                OctetsFW fetchKey = beginEx.fetchKey();
                byte hashCodesCount = beginEx.fetchKeyHashCount();
                if ((fetchKey != null && this.fetchOffsets.size() > 1) ||
                    (hashCodesCount > 1) ||
                    (hashCodesCount == 1 && fetchKey == null))
                {
                    doReset(applicationThrottle, applicationId);
                }
                else
                {
                    ListFW<KafkaHeaderFW> headers = beginEx.headers();
                    if (headers != null && headers.sizeof() > 0)
                    {
                        MutableDirectBuffer headersBuffer = new UnsafeBuffer(new byte[headers.limit() - headers.offset()]);
                        headersBuffer.putBytes(0, headers.buffer(),  headers.offset(), headers.sizeof());
                        headers = new ListFW<KafkaHeaderFW>(new KafkaHeaderFW()).wrap(headersBuffer, 0, headersBuffer.capacity());
                    }
                    int hashCode = -1;
                    if (fetchKey != null)
                    {
                        subscribedByKey = true;
                        MutableDirectBuffer keyBuffer = new UnsafeBuffer(new byte[fetchKey.limit() - fetchKey.offset()]);
                        keyBuffer.putBytes(0, fetchKey.buffer(),  fetchKey.offset(), fetchKey.sizeof());
                        fetchKey = new OctetsFW().wrap(keyBuffer, 0, keyBuffer.capacity());
                        hashCode = hashCodesCount == 1 ? beginEx.fetchKeyHash().nextInt()
                                : BufferUtil.defaultHashCode(fetchKey.buffer(), fetchKey.offset(), fetchKey.limit());
                    }
                    networkPool.doAttach(topicName, this.fetchOffsets, hashCode, fetchKey, routeHeaders, headers,
                            this, this::writeableBytes, h -> progressHandler = h, this::onAttachPrepared, this::onMetadataError);
                    this.streamState = this::afterBegin;
                }
            }
        }

        private void onAttachPrepared(
            Consumer<IntBooleanConsumer> attacher)
        {
            this.attacher = attacher;
            final long newReplyId = supplyStreamId.getAsLong();
            final String replyName = applicationName;
            final MessageConsumer newReply = router.supplyTarget(replyName);

            doKafkaBegin(newReply, newReplyId, 0L, applicationCorrelationId, applicationBeginExtension);
            router.setThrottle(applicationName, newReplyId, this::handleThrottle);

            doWindow(applicationThrottle, applicationId, 0, 0, 0);
            this.applicationReply = newReply;
            this.applicationReplyId = newReplyId;
        }

        private void onAttached(
            int newNetworkAttachId,
            boolean compacted)
        {
            this.networkAttachId = newNetworkAttachId;
            this.compacted = compacted;
        }

        private void onMetadataError(
            int errorCode)
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
            applicationReplyPadding = window.padding();
            groupId = window.groupId();
            if (firstWindow)
            {
                if (groupId != 0)
                {
                    budget = new GroupBudget(groupId, window.streamId(), networkPool);
                }
                firstWindow = false;
                budget.joinGroup(window.credit());
                if (attacher != null)
                {
                    attacher.accept(this::onAttached);
                    attacher = null;
                }
            }
            else
            {
                budget.incApplicationReplyBudget(window.credit());
            }
            networkPool.doFlush();
        }

        private boolean equals(DirectBuffer buffer1, DirectBuffer buffer2)
        {
            boolean result = false;
            if (buffer1 == buffer2)
            {
                result = true;
            }
            else if (buffer1 == null || buffer2 == null)
            {
                result = false;
            }
            else
            {
                workBuffer1.wrap(buffer1);
                workBuffer2.wrap(buffer2);
                result = workBuffer1.equals(workBuffer2);
            }
            return result;
        }

        private void handleReset(
            ResetFW reset)
        {
            networkPool.doDetach(networkAttachId, fetchOffsets);
            networkAttachId = UNATTACHED;
            budget.leaveGroup();
            doReset(applicationThrottle, applicationId);
        }

        private DirectBuffer wrap(DirectBuffer wrapper, DirectBuffer wrapped)
        {
            DirectBuffer result = null;
            if (wrapped != null)
            {
                wrapper.wrap(wrapped, 0, wrapped.capacity());
                result = wrapper;
            }
            return result;
        }

        private int writeableBytes()
        {
            final int writeableBytes = budget.applicationReplyBudget() - applicationReplyPadding;
            return writeableBytes >= writeableBytesMinimum ? writeableBytes : 0;
        }

    }


    private interface Budget
    {
        int applicationReplyBudget();

        void applicationReplyBudget(int budget);

        void decApplicationReplyBudget(int data);

        void incApplicationReplyBudget(int data);

        void joinGroup(int credit);

        void leaveGroup();
    }

    private final class StreamBudget implements Budget
    {
        int applicationReplyBudget;

        @Override
        public int applicationReplyBudget()
        {
            return applicationReplyBudget;
        }

        @Override
        public void applicationReplyBudget(int budget)
        {
            applicationReplyBudget = budget;
        }

        @Override
        public void decApplicationReplyBudget(int data)
        {
            applicationReplyBudget -= data;
            assert applicationReplyBudget >= 0;
        }

        @Override
        public void incApplicationReplyBudget(int credit)
        {
            applicationReplyBudget += credit;
        }

        @Override
        public void joinGroup(int credit)
        {
            applicationReplyBudget += credit;
        }

        @Override
        public void leaveGroup()
        {

        }
    }

    private final class GroupBudget implements Budget
    {
        private final long groupId;
        private final long streamId;
        private final NetworkConnectionPool networkPool;
        private int uncreditedBudget;

        GroupBudget(long groupId, long streamId, NetworkConnectionPool networkPool)
        {
            this.groupId = groupId;
            this.streamId = streamId;
            this.networkPool = networkPool;
        }

        @Override
        public int applicationReplyBudget()
        {
            long budget = groupBudget.get(groupId);
            return budget == -1 ? 0 : (int) budget;
        }

        @Override
        public void applicationReplyBudget(int budget)
        {
            assert groupBudget.containsKey(groupId);
            assert budget >= 0;

            groupBudget.put(groupId, budget);
        }

        @Override
        public void decApplicationReplyBudget(int data)
        {
            assert groupBudget.containsKey(groupId);

            int budget = applicationReplyBudget();
            assert budget - data >= 0;
            applicationReplyBudget(budget - data);

            uncreditedBudget += data;
        }

        @Override
        public void incApplicationReplyBudget(int credit)
        {
            assert groupBudget.containsKey(groupId);

            int budget = applicationReplyBudget();
            applicationReplyBudget(budget + credit);

            uncreditedBudget -= credit;
        }

        @Override
        public void joinGroup(int credit)
        {
            long memberCount = groupMembers.get(groupId);
            memberCount = (memberCount == -1) ? 1 : memberCount + 1;
            groupMembers.put(groupId, memberCount);

            if (memberCount == 1)
            {
                assert !groupBudget.containsKey(groupId);
                groupBudget.put(groupId, credit);
            }
        }

        @Override
        public void leaveGroup()
        {
            long memberCount = groupMembers.get(groupId);
            if (memberCount != -1)
            {
                memberCount--;
                assert memberCount >= 0;

                if (memberCount == 0)
                {
                    groupMembers.remove(groupId);
                    groupBudget.remove(groupId);
                }
                else
                {
                    groupMembers.put(groupId, memberCount);

                    assert groupBudget.containsKey(groupId);
                    int budget = applicationReplyBudget();
                    applicationReplyBudget(budget + uncreditedBudget);
                    uncreditedBudget = 0;

                    // more (from uncredited) group budget available, some messages can be sent
                    networkPool.doFlush();
                }
            }
        }

        @Override
        public String toString()
        {
            return String.format("(groupId=%d, streamId=%d)", groupId, streamId);
        }
    }
}
