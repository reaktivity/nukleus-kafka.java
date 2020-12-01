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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.concurrent.Signaler.NO_CANCEL_ID;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorNextValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorRetryValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorValue;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW.Builder.DEFAULT_LATEST_OFFSET;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType.LIVE;
import static org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW.Builder.DEFAULT_DEFAULT_OFFSET;
import static org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW.Builder.DEFAULT_DELTA_TYPE;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

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
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheIndexFile;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartition;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartition.Node;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheTopic;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFetchDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaResetExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaCacheServerFetchFactory implements StreamFactory
{
    static final int SIZE_OF_FLUSH_WITH_EXTENSION = 64;

    private static final int ERROR_NOT_LEADER_FOR_PARTITION = 6;

    private static final DirectBuffer EMPTY_BUFFER = new UnsafeBuffer();
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(EMPTY_BUFFER, 0, 0);
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};

    private static final int FLAGS_INIT = 0x02;
    private static final int FLAGS_FIN = 0x01;

    private static final int SIGNAL_RECONNECT = 1;
    private static final int SIGNAL_SEGMENT_RETAIN = 2;
    private static final int SIGNAL_SEGMENT_DELETE = 3;
    private static final int SIGNAL_SEGMENT_COMPACT = 4;

    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

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
    private final KafkaResetExFW kafkaResetExRO = new KafkaResetExFW();
    private final KafkaResetExFW.Builder kafkaResetExRW = new KafkaResetExFW.Builder();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final FlushFW.Builder flushRW = new FlushFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();
    private final KafkaFlushExFW.Builder kafkaFlushExRW = new KafkaFlushExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final KafkaCacheEntryFW ancestorEntryRO = new KafkaCacheEntryFW();

    private final int kafkaTypeId;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final BufferPool bufferPool;
    private final Signaler signaler;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final Function<String, KafkaCache> supplyCache;
    private final LongFunction<KafkaCacheRoute> supplyCacheRoute;
    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final int reconnectDelay;

    public KafkaCacheServerFetchFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        Signaler signaler,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        Function<String, KafkaCache> supplyCache,
        LongFunction<KafkaCacheRoute> supplyCacheRoute,
        Long2ObjectHashMap<MessageConsumer> correlations)
    {
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.router = router;
        this.writeBuffer = writeBuffer;
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = bufferPool;
        this.signaler = signaler;
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.supplyTraceId = supplyTraceId;
        this.supplyCache = supplyCache;
        this.supplyCacheRoute = supplyCacheRoute;
        this.correlations = correlations;
        this.reconnectDelay = config.cacheServerReconnect();
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
        final long routeId = begin.routeId();
        final long initialId = begin.streamId();
        final long affinity = begin.affinity();
        final long authorization = begin.authorization();

        assert (initialId & 0x0000_0000_0000_0001L) != 0L;

        final OctetsFW extension = begin.extension();
        final ExtensionFW beginEx = extension.get(extensionRO::wrap);
        assert beginEx != null && beginEx.typeId() == kafkaTypeId;
        final KafkaBeginExFW kafkaBeginEx = extension.get(kafkaBeginExRO::wrap);
        final KafkaFetchBeginExFW kafkaFetchBeginEx = kafkaBeginEx.fetch();

        final String16FW beginTopic = kafkaFetchBeginEx.topic();
        final KafkaOffsetFW progress = kafkaFetchBeginEx.partition();
        final int partitionId = progress.partitionId();
        final long partitionOffset = progress.partitionOffset();
        final KafkaDeltaType deltaType = kafkaFetchBeginEx.deltaType().get();

        MessageConsumer newStream = null;

        final MessagePredicate filter = (t, b, i, l) ->
        {
            final RouteFW route = wrapRoute.apply(t, b, i, l);
            final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
            final String16FW routeTopic = routeEx != null ? routeEx.topic() : null;
            final KafkaDeltaType routeDeltaType = routeEx != null ? routeEx.deltaType().get() : DEFAULT_DELTA_TYPE;
            return !route.localAddress().equals(route.remoteAddress()) &&
                    (beginTopic != null && (routeTopic == null || routeTopic.equals(beginTopic))) &&
                    (routeDeltaType == deltaType || deltaType == KafkaDeltaType.NONE);
        };

        final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
        if (route != null)
        {
            final String topicName = beginTopic.asString();
            final long resolvedId = route.correlationId();
            final KafkaCacheRoute cacheRoute = supplyCacheRoute.apply(resolvedId);
            final long partitionKey = cacheRoute.topicPartitionKey(topicName, partitionId);

            KafkaCacheServerFetchFanout fanout = cacheRoute.serverFetchFanoutsByTopicPartition.get(partitionKey);
            if (fanout == null)
            {
                final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
                final KafkaDeltaType routeDeltaType = routeEx != null ? routeEx.deltaType().get() : DEFAULT_DELTA_TYPE;
                final KafkaOffsetType defaultOffset = routeEx != null ? routeEx.defaultOffset().get() : DEFAULT_DEFAULT_OFFSET;
                final String cacheName = route.localAddress().asString();
                final KafkaCache cache = supplyCache.apply(cacheName);
                final KafkaCacheTopic topic = cache.supplyTopic(topicName);
                final KafkaCachePartition partition = topic.supplyFetchPartition(partitionId);
                final KafkaCacheServerFetchFanout newFanout = new KafkaCacheServerFetchFanout(resolvedId, authorization,
                        affinity, partition, routeDeltaType, defaultOffset);

                cacheRoute.serverFetchFanoutsByTopicPartition.put(partitionKey, newFanout);
                fanout = newFanout;
            }

            final int leaderId = cacheRoute.leadersByPartitionId.get(partitionId);

            newStream = new KafkaCacheServerFetchStream(
                    fanout,
                    sender,
                    routeId,
                    initialId,
                    leaderId,
                    authorization,
                    partitionOffset)::onServerMessage;
        }

        return newStream;
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

    private void doFlush(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        Consumer<OctetsFW.Builder> extension)
    {
        final FlushFW flush = flushRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .reserved(reserved)
                .extension(extension)
                .build();

        receiver.accept(flush.typeId(), flush.buffer(), flush.offset(), flush.sizeof());
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

    final class KafkaCacheServerFetchFanout
    {
        private final long routeId;
        private final long authorization;
        private final KafkaCachePartition partition;
        private final KafkaDeltaType deltaType;
        private final KafkaOffsetType defaultOffset;
        private final long retentionMillisMax;
        private final List<KafkaCacheServerFetchStream> members;

        private long leaderId;
        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private long partitionOffset;
        private long latestOffset = DEFAULT_LATEST_OFFSET;
        private long retainId = NO_CANCEL_ID;
        private long deleteId = NO_CANCEL_ID;
        private long compactId = NO_CANCEL_ID;
        private long compactAt = Long.MAX_VALUE;
        private long reconnectAt = NO_CANCEL_ID;
        private int reconnectAttempt;

        private KafkaCacheServerFetchFanout(
            long routeId,
            long authorization,
            long leaderId,
            KafkaCachePartition partition,
            KafkaDeltaType deltaType,
            KafkaOffsetType defaultOffset)
        {
            this.routeId = routeId;
            this.authorization = authorization;
            this.partition = partition;
            this.deltaType = deltaType;
            this.defaultOffset = defaultOffset;
            this.retentionMillisMax = defaultOffset == LIVE ? SECONDS.toMillis(30) : Long.MAX_VALUE;
            this.members = new ArrayList<>();
            this.leaderId = leaderId;
        }

        private void onServerFanoutMemberOpening(
            long traceId,
            KafkaCacheServerFetchStream member)
        {
            if (member.leaderId != leaderId)
            {
                correlations.remove(replyId);
                doServerFanoutInitialAbortIfNecessary(traceId);
                doServerFanoutReplyResetIfNecessary(traceId);
                leaderId = member.leaderId;

                members.forEach(m -> m.cleanupServer(traceId, ERROR_NOT_LEADER_FOR_PARTITION));
                members.clear();
            }

            members.add(member);

            assert !members.isEmpty();

            doServerFanoutInitialBeginIfNecessary(traceId);

            if (KafkaState.initialOpened(state))
            {
                member.doServerInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }

            if (KafkaState.replyOpened(state))
            {
                member.doServerReplyBeginIfNecessary(traceId);
            }
        }

        private void onServerFanoutMemberClosed(
            long traceId,
            KafkaCacheServerFetchStream member)
        {
            members.remove(member);

            if (members.isEmpty())
            {
                if (reconnectAt != NO_CANCEL_ID)
                {
                    signaler.cancel(reconnectAt);
                    this.reconnectAt = NO_CANCEL_ID;
                }

                correlations.remove(replyId);
                doServerFanoutInitialAbortIfNecessary(traceId);
                doServerFanoutReplyResetIfNecessary(traceId);
            }
        }

        private void doServerFanoutInitialBeginIfNecessary(
            long traceId)
        {
            if (KafkaState.closed(state))
            {
                state = 0;
            }

            if (!KafkaState.initialOpening(state))
            {
                doServerFanoutInitialBegin(traceId);
            }
        }

        private void doServerFanoutInitialBegin(
            long traceId)
        {
            assert state == 0;

            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            if (KafkaConfiguration.DEBUG)
            {
                System.out.format("[0x%016x] %s FETCH connect, affinity %d\n", initialId, partition, leaderId);
            }

            this.partitionOffset = partition.nextOffset(defaultOffset);

            correlations.put(replyId, this::onServerFanoutMessage);
            router.setThrottle(initialId, this::onServerFanoutMessage);
            doBegin(receiver, routeId, initialId, traceId, authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.topic(partition.topic())
                                     .partition(p -> p.partitionId(partition.id())
                                                      .partitionOffset(partitionOffset)))
                        .build()
                        .sizeof()));
            state = KafkaState.openingInitial(state);
        }

        private void doServerFanoutInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doServerFanoutInitialEnd(traceId);
            }
        }

        private void doServerFanoutInitialEnd(
            long traceId)
        {
            doEnd(receiver, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);

            state = KafkaState.closedInitial(state);
        }

        private void doServerFanoutInitialAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doServerFanoutInitialAbort(traceId);
            }
        }

        private void doServerFanoutInitialAbort(
            long traceId)
        {
            doAbort(receiver, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);

            state = KafkaState.closedInitial(state);
        }

        private void onServerFanoutMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onServerFanoutReplyBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onServerFanoutReplyData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onServerFanoutReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onServerFanoutReplyAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onServerFanoutInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onServerFanoutInitialWindow(window);
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                onServerFanoutInitialSignal(signal);
                break;
            default:
                break;
            }
        }

        private void onServerFanoutReplyBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final OctetsFW extension = begin.extension();
            final ExtensionFW beginEx = extension.get(extensionRO::tryWrap);
            assert beginEx != null && beginEx.typeId() == kafkaTypeId;
            final KafkaBeginExFW kafkaBeginEx = extension.get(kafkaBeginExRO::wrap);
            assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_FETCH;
            final KafkaFetchBeginExFW kafkaFetchBeginEx = kafkaBeginEx.fetch();
            final KafkaOffsetFW progress = kafkaFetchBeginEx.partition();
            final int partitionId = progress.partitionId();
            final long partitionOffset = progress.partitionOffset();

            state = KafkaState.openedReply(state);

            assert partitionId == partition.id();
            assert partitionOffset >= 0L && partitionOffset >= this.partitionOffset;
            this.partitionOffset = partitionOffset;
            this.latestOffset = progress.latestOffset();

            partition.newHeadIfNecessary(partitionOffset);

            members.forEach(s -> s.doServerReplyBeginIfNecessary(traceId));

            doServerFanoutReplyWindow(traceId, bufferPool.slotCapacity());
        }

        private void onServerFanoutReplyData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int reserved = data.reserved();
            final int flags = data.flags();
            final OctetsFW valueFragment = data.payload();

            KafkaFetchDataExFW kafkaFetchDataEx = null;
            if ((flags & (FLAGS_INIT | FLAGS_FIN)) != 0x00)
            {
                final OctetsFW extension = data.extension();
                final ExtensionFW dataEx = extension.get(extensionRO::tryWrap);
                assert dataEx != null && dataEx.typeId() == kafkaTypeId;
                final KafkaDataExFW kafkaDataEx = extension.get(kafkaDataExRO::wrap);
                assert kafkaDataEx.kind() == KafkaDataExFW.KIND_FETCH;
                kafkaFetchDataEx = kafkaDataEx.fetch();
            }

            if ((flags & FLAGS_INIT) != 0x00)
            {
                assert kafkaFetchDataEx != null;
                final int deferred = kafkaFetchDataEx.deferred();
                final int partitionId = kafkaFetchDataEx.partition().partitionId();
                final long partitionOffset = kafkaFetchDataEx.partition().partitionOffset();
                final int headersSizeMax = Math.max(kafkaFetchDataEx.headers().sizeof(), kafkaFetchDataEx.headersSizeMax());
                final long timestamp = kafkaFetchDataEx.timestamp();
                final KafkaKeyFW key = kafkaFetchDataEx.key();
                final KafkaDeltaFW delta = kafkaFetchDataEx.delta();
                final int valueLength = valueFragment != null ? valueFragment.sizeof() + deferred : -1;

                assert delta.type().get() == KafkaDeltaType.NONE;
                assert delta.ancestorOffset() == -1L;
                assert partitionId == partition.id();
                assert partitionOffset >= this.partitionOffset : String.format("%d >= %d", partitionOffset, this.partitionOffset);

                final KafkaCachePartition.Node head = partition.head();
                final KafkaCachePartition.Node nextHead =
                        partition.newHeadIfNecessary(partitionOffset, key, valueLength, headersSizeMax);

                final long nextOffset = partition.nextOffset(defaultOffset);
                assert partitionOffset >= 0 && partitionOffset >= nextOffset
                        : String.format("%d >= 0 && %d >= %d", partitionOffset, partitionOffset, nextOffset);

                if (nextHead != head)
                {
                    if (retainId != NO_CANCEL_ID)
                    {
                        signaler.cancel(retainId);
                        this.retainId = NO_CANCEL_ID;
                    }

                    assert retainId == NO_CANCEL_ID;

                    final long retainAt = partition.retainAt(nextHead.segment());
                    this.retainId = doServerFanoutInitialSignalAt(retainAt, SIGNAL_SEGMENT_RETAIN);

                    if (deleteId == NO_CANCEL_ID &&
                        partition.cleanupPolicy().delete() &&
                        !nextHead.previous().sentinel())
                    {
                        final long deleteAt = partition.deleteAt(nextHead.previous().segment(), retentionMillisMax);
                        this.deleteId = doServerFanoutInitialSignalAt(deleteAt, SIGNAL_SEGMENT_DELETE);
                    }
                }

                final long keyHash = partition.computeKeyHash(key);
                final KafkaCacheEntryFW ancestor = findAndMarkAncestor(key, nextHead, (int) keyHash, partitionOffset);
                partition.writeEntryStart(partitionOffset, timestamp, key, keyHash, valueLength, ancestor, deltaType);
            }

            if (valueFragment != null)
            {
                partition.writeEntryContinue(valueFragment);
            }

            if ((flags & FLAGS_FIN) != 0x00)
            {
                assert kafkaFetchDataEx != null;
                final int partitionId = kafkaFetchDataEx.partition().partitionId();
                final long partitionOffset = kafkaFetchDataEx.partition().partitionOffset();
                final long latestOffset = kafkaFetchDataEx.partition().latestOffset();
                final KafkaDeltaFW delta = kafkaFetchDataEx.delta();
                final ArrayFW<KafkaHeaderFW> headers = kafkaFetchDataEx.headers();

                assert delta.type().get() == KafkaDeltaType.NONE;
                assert delta.ancestorOffset() == -1L;
                assert partitionId == partition.id();
                assert partitionOffset >= this.partitionOffset;

                partition.writeEntryFinish(headers, deltaType);

                this.partitionOffset = partitionOffset;
                this.latestOffset = latestOffset;

                members.forEach(s -> s.doServerReplyFlushIfNecessary(traceId));
            }

            doServerFanoutReplyWindow(traceId, reserved);
        }

        private KafkaCacheEntryFW findAndMarkAncestor(
            KafkaKeyFW key,
            KafkaCachePartition.Node head,
            int keyHash,
            long descendantOffset)
        {
            KafkaCacheEntryFW ancestorEntry = null;
            ancestor:
            if (key.length() != -1)
            {
                ancestorEntry = head.findAndMarkAncestor(key, keyHash, descendantOffset, ancestorEntryRO);
                if (ancestorEntry != null)
                {
                    if (partition.cleanupPolicy().compact())
                    {
                        final long newCompactAt = partition.compactAt(head.segment());
                        if (newCompactAt != Long.MAX_VALUE)
                        {
                            if (compactId != NO_CANCEL_ID && newCompactAt < compactAt)
                            {
                                signaler.cancel(compactId);
                                this.compactId = NO_CANCEL_ID;
                            }

                            if (compactId == NO_CANCEL_ID)
                            {
                                this.compactAt = newCompactAt;
                                this.compactId = doServerFanoutInitialSignalAt(newCompactAt, SIGNAL_SEGMENT_COMPACT);
                            }
                        }
                    }
                    break ancestor;
                }

                Node previousNode = head.previous();
                while (!previousNode.sentinel())
                {
                    final KafkaCacheSegment previousSegment = previousNode.segment();
                    final KafkaCacheIndexFile previousKeys = previousSegment.keysFile();

                    long keyCursor = previousKeys.last(keyHash);
                    while (!cursorNextValue(keyCursor) && !cursorRetryValue(keyCursor))
                    {
                        final int keyBaseOffsetDelta = cursorValue(keyCursor);
                        assert keyBaseOffsetDelta <= 0;
                        final long keyBaseOffset = previousSegment.baseOffset() + keyBaseOffsetDelta;
                        final Node ancestorNode = previousNode.seekAncestor(keyBaseOffset);
                        if (!ancestorNode.sentinel())
                        {
                            final KafkaCacheSegment segment = ancestorNode.segment();
                            final long ancestorBase = segment.baseOffset();
                            assert ancestorBase == keyBaseOffset : String.format("%d == %d", ancestorBase, keyBaseOffset);
                            ancestorEntry = ancestorNode.findAndMarkAncestor(key, keyHash, descendantOffset, ancestorEntryRO);
                            if (ancestorEntry != null)
                            {
                                if (partition.cleanupPolicy().compact())
                                {
                                    final long newCompactAt = partition.compactAt(segment);
                                    if (newCompactAt != Long.MAX_VALUE)
                                    {
                                        if (compactId != NO_CANCEL_ID && newCompactAt < compactAt)
                                        {
                                            signaler.cancel(compactId);
                                            this.compactId = NO_CANCEL_ID;
                                        }

                                        if (compactId == NO_CANCEL_ID)
                                        {
                                            this.compactAt = newCompactAt;
                                            this.compactId = doServerFanoutInitialSignalAt(newCompactAt, SIGNAL_SEGMENT_COMPACT);
                                        }
                                    }
                                }
                                break ancestor;
                            }
                        }

                        final long nextKeyCursor = previousKeys.lower(keyHash, keyCursor);
                        if (cursorNextValue(nextKeyCursor) || cursorRetryValue(nextKeyCursor))
                        {
                            break;
                        }

                        keyCursor = nextKeyCursor;
                    }

                    previousNode = previousNode.previous();
                }
            }
            return ancestorEntry;
        }

        private void onServerFanoutReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            doServerFanoutInitialEndIfNecessary(traceId);

            if (reconnectDelay != 0 && !members.isEmpty())
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[0x%016x] %s FETCH reconnect in %ds\n", initialId, partition, reconnectDelay);
                }

                if (reconnectAt != NO_CANCEL_ID)
                {
                    signaler.cancel(reconnectAt);
                }

                this.reconnectAt = signaler.signalAt(
                    currentTimeMillis() + Math.min(50 << reconnectAttempt++, SECONDS.toMillis(reconnectDelay)),
                    SIGNAL_RECONNECT,
                    this::onServerFanoutSignal);
            }
            else
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[0x%016x] %s FETCH disconnect\n", initialId, partition);
                }

                members.forEach(s -> s.doServerReplyEndIfNecessary(traceId));
            }
        }

        private void onServerFanoutReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            doServerFanoutInitialAbortIfNecessary(traceId);

            if (reconnectDelay != 0 && !members.isEmpty())
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[0x%016x] %s FETCH reconnect in %ds\n", initialId, partition, reconnectDelay);
                }

                if (reconnectAt != NO_CANCEL_ID)
                {
                    signaler.cancel(reconnectAt);
                }

                this.reconnectAt = signaler.signalAt(
                    currentTimeMillis() + Math.min(50 << reconnectAttempt++, SECONDS.toMillis(reconnectDelay)),
                    SIGNAL_RECONNECT,
                    this::onServerFanoutSignal);
            }
            else
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[0x%016x] %s FETCH disconnect\n", initialId, partition);
                }

                members.forEach(s -> s.doServerReplyAbortIfNecessary(traceId));
            }
        }

        private void onServerFanoutInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final OctetsFW extension = reset.extension();

            state = KafkaState.closedInitial(state);

            doServerFanoutReplyResetIfNecessary(traceId);

            final KafkaResetExFW kafkaResetEx = extension.get(kafkaResetExRO::tryWrap);
            final int error = kafkaResetEx != null ? kafkaResetEx.error() : -1;

            if (reconnectDelay != 0 && !members.isEmpty() &&
                error != ERROR_NOT_LEADER_FOR_PARTITION)
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[0x%016x] %s FETCH reconnect in %ds, error %d \n", initialId, partition, reconnectDelay,
                        error);
                }

                if (reconnectAt != NO_CANCEL_ID)
                {
                    signaler.cancel(reconnectAt);
                }

                this.reconnectAt = signaler.signalAt(
                    currentTimeMillis() + Math.min(50 << reconnectAttempt++, SECONDS.toMillis(reconnectDelay)),
                    SIGNAL_RECONNECT,
                    this::onServerFanoutSignal);
            }
            else
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("[0x%016x] %s FETCH disconnect, error %d\n", initialId, partition, error);
                }

                members.forEach(s -> s.doServerInitialResetIfNecessary(traceId, extension));
            }
        }

        private void onServerFanoutInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                this.reconnectAttempt = 0;

                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                members.forEach(s -> s.doServerInitialWindowIfNecessary(traceId, 0L, 0, 0));
            }
        }

        private void onServerFanoutSignal(
            int signalId)
        {
            assert signalId == SIGNAL_RECONNECT;

            this.reconnectAt = NO_CANCEL_ID;

            final long traceId = supplyTraceId.getAsLong();

            doServerFanoutInitialBeginIfNecessary(traceId);
        }

        private void onServerFanoutInitialSignal(
            SignalFW signal)
        {
            final int signalId = signal.signalId();

            switch (signalId)
            {
            case SIGNAL_SEGMENT_RETAIN:
                onServerFanoutInitialSignalSegmentRetain(signal);
                break;
            case SIGNAL_SEGMENT_DELETE:
                onServerFanoutInitialSignalSegmentDelete(signal);
                break;
            case SIGNAL_SEGMENT_COMPACT:
                onServerFanoutInitialSignalSegmentCompact(signal);
                break;
            }
        }

        private void onServerFanoutInitialSignalSegmentRetain(
            SignalFW signal)
        {
            partition.append(partitionOffset + 1);
        }

        private void onServerFanoutInitialSignalSegmentDelete(
            SignalFW signal)
        {
            final long now = currentTimeMillis();

            Node segmentNode = partition.sentinel().next();
            while (segmentNode != partition.head() &&
                    partition.deleteAt(segmentNode.segment(), retentionMillisMax) <= now)
            {
                segmentNode.remove();
                segmentNode = segmentNode.next();
            }
            assert segmentNode != null;

            if (segmentNode != partition.head())
            {
                final long deleteAt = partition.deleteAt(segmentNode.segment(), retentionMillisMax);
                this.deleteId = doServerFanoutInitialSignalAt(deleteAt, SIGNAL_SEGMENT_DELETE);
            }
            else
            {
                this.deleteId = NO_CANCEL_ID;
            }
        }

        private void onServerFanoutInitialSignalSegmentCompact(
            SignalFW signal)
        {
            final long now = currentTimeMillis();

            Node segmentNode = partition.sentinel().next();
            while (!segmentNode.next().sentinel()) // avoid cleaning head
            {
                segmentNode.clean(now);
                segmentNode = segmentNode.next();
            }

            this.compactAt = Long.MAX_VALUE;
            this.compactId = NO_CANCEL_ID;
        }

        private void doServerFanoutReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doServerFanoutReplyReset(traceId);
            }
        }

        private void doServerFanoutReplyReset(
            long traceId)
        {
            correlations.remove(replyId);

            state = KafkaState.closedReply(state);

            doReset(receiver, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doServerFanoutReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            doWindow(receiver, routeId, replyId, traceId, authorization, 0L, credit, 0);
        }

        private long doServerFanoutInitialSignalAt(
            long timeMillis,
            int signalId)
        {
            return signaler.signalAt(timeMillis, routeId, initialId, signalId);
        }
    }

    private final class KafkaCacheServerFetchStream
    {
        private final KafkaCacheServerFetchFanout group;
        private final MessageConsumer sender;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long leaderId;
        private final long authorization;

        private int state;

        private int replyBudget;
        private long partitionOffset;
        private long latestOffset;

        KafkaCacheServerFetchStream(
            KafkaCacheServerFetchFanout group,
            MessageConsumer sender,
            long routeId,
            long initialId,
            long leaderId,
            long authorization,
            long partitionOffset)
        {
            this.group = group;
            this.sender = sender;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.leaderId = leaderId;
            this.authorization = authorization;
            this.partitionOffset = partitionOffset;
        }

        private void onServerMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onServerInitialBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onServerInitialData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onServerInitialEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onServerInitialAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onServerReplyWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onServerReplyReset(reset);
                break;
            default:
                break;
            }
        }

        private void onServerInitialBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long affinity = begin.affinity();

            if (affinity != leaderId)
            {
                cleanupServer(traceId, ERROR_NOT_LEADER_FOR_PARTITION);
            }
            else
            {
                state = KafkaState.openingInitial(state);
                group.onServerFanoutMemberOpening(traceId, this);
            }
        }

        private void onServerInitialData(
            DataFW data)
        {
            final long traceId = data.traceId();

            doServerInitialResetIfNecessary(traceId, EMPTY_OCTETS);
            doServerReplyAbortIfNecessary(traceId);

            group.onServerFanoutMemberClosed(traceId, this);
        }

        private void onServerInitialEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedInitial(state);

            group.onServerFanoutMemberClosed(traceId, this);

            doServerReplyEndIfNecessary(traceId);
        }

        private void onServerInitialAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedInitial(state);

            group.onServerFanoutMemberClosed(traceId, this);

            doServerReplyAbortIfNecessary(traceId);
        }

        private void doServerInitialResetIfNecessary(
            long traceId,
            Flyweight extension)
        {
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doServerInitialReset(traceId, extension);
            }

            state = KafkaState.closedInitial(state);
        }

        private void doServerInitialReset(
            long traceId,
            Flyweight extension)
        {
            state = KafkaState.closedInitial(state);

            doReset(sender, routeId, initialId, traceId, authorization, extension);
        }

        private void doServerInitialWindowIfNecessary(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            if (!KafkaState.initialOpened(state) || credit > 0)
            {
                doServerInitialWindow(traceId, budgetId, credit, padding);
            }
        }

        private void doServerInitialWindow(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            state = KafkaState.openedInitial(state);

            doWindow(sender, routeId, initialId, traceId, authorization,
                    budgetId, credit, padding);
        }

        private void doServerReplyBeginIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyOpening(state))
            {
                doServerReplyBegin(traceId);
            }
        }

        private void doServerReplyBegin(
            long traceId)
        {
            state = KafkaState.openingReply(state);

            this.partitionOffset = Math.max(partitionOffset, group.partitionOffset);

            router.setThrottle(replyId, this::onServerMessage);
            doBegin(sender, routeId, replyId, traceId, authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.topic(group.partition.topic())
                                     .partition(p -> p.partitionId(group.partition.id())
                                                      .partitionOffset(partitionOffset)
                                                      .latestOffset(group.latestOffset)))
                        .build()
                        .sizeof()));
        }

        private void doServerReplyFlushIfNecessary(
            long traceId)
        {
            if ((partitionOffset <= group.partitionOffset || latestOffset <= group.latestOffset) &&
                replyBudget >= SIZE_OF_FLUSH_WITH_EXTENSION)
            {
                doServerReplyFlush(traceId, SIZE_OF_FLUSH_WITH_EXTENSION);
            }
        }

        private void doServerReplyFlush(
            long traceId,
            int reserved)
        {
            assert partitionOffset <= group.partitionOffset || latestOffset <= group.latestOffset;

            replyBudget -= reserved;

            assert replyBudget >= 0;

            doFlush(sender, routeId, replyId, traceId, authorization, 0L, reserved,
                ex -> ex.set((b, o, l) -> kafkaFlushExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.partition(p -> p.partitionId(group.partition.id())
                                                      .partitionOffset(group.partitionOffset)
                                                      .latestOffset(group.latestOffset)))
                        .build()
                        .sizeof()));

            this.partitionOffset = group.partitionOffset + 1;
            this.latestOffset = group.latestOffset + 1;
        }

        private void doServerReplyEndIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doServerReplyEnd(traceId);
            }

            state = KafkaState.closedReply(state);
        }

        private void doServerReplyEnd(
                long traceId)
        {
            state = KafkaState.closedReply(state);
            doEnd(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doServerReplyAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doServerReplyAbort(traceId);
            }

            state = KafkaState.closedReply(state);
        }

        private void doServerReplyAbort(
                long traceId)
        {
            state = KafkaState.closedReply(state);
            doAbort(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void onServerReplyWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            assert budgetId == 0L;
            assert padding == 0;

            state = KafkaState.openedReply(state);

            replyBudget += credit;

            doServerReplyFlushIfNecessary(traceId);
        }

        private void onServerReplyReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedReply(state);

            group.onServerFanoutMemberClosed(traceId, this);

            doServerInitialResetIfNecessary(traceId, EMPTY_OCTETS);
        }

        private void cleanupServer(
            long traceId,
            int error)
        {
            final KafkaResetExFW kafkaResetEx = kafkaResetExRW.wrap(extBuffer, 0, extBuffer.capacity())
                                                              .typeId(kafkaTypeId)
                                                              .error(error)
                                                              .build();

            cleanupServer(traceId, kafkaResetEx);
        }

        private void cleanupServer(
            long traceId,
            Flyweight extension)
        {
            doServerInitialReset(traceId, extension);
            doServerReplyAbortIfNecessary(traceId);
        }
    }
}
