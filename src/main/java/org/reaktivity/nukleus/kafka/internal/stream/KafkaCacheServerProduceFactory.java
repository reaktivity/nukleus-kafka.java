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
import static org.reaktivity.nukleus.concurrent.Signaler.NO_CANCEL_ID;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW.Builder.DEFAULT_LATEST_OFFSET;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorFactory;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorFactory.KafkaCacheCursor;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartition;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheTopic;
import org.reaktivity.nukleus.kafka.internal.types.Array32FW;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaFilterFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaProduceBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaProduceFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaResetExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.reaktor.ReaktorConfiguration;

public final class KafkaCacheServerProduceFactory implements StreamFactory
{
    private static final int ERROR_NOT_LEADER_FOR_PARTITION = 6;

    private static final String TRANSACTION_NONE = null;

    private static final int SIZE_OF_FLUSH_WITH_EXTENSION = 64;

    private static final int FLAG_FIN = 0x01;
    private static final int FLAG_INIT = 0x02;
    private static final int FLAG_NONE = 0x00;

    private static final DirectBuffer EMPTY_BUFFER = new UnsafeBuffer();
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(EMPTY_BUFFER, 0, 0);
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};
    private static final Array32FW<KafkaFilterFW> EMPTY_FILTER =
        new Array32FW.Builder<>(new KafkaFilterFW.Builder(), new KafkaFilterFW())
            .wrap(new UnsafeBuffer(new byte[64]), 0, 64).build();

    private static final int SIGNAL_RECONNECT = 1;

    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final FlushFW flushRO = new FlushFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();
    private final KafkaResetExFW kafkaResetExRO = new KafkaResetExFW();
    private final KafkaFlushExFW kafkaFlushExRO = new KafkaFlushExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final FlushFW.Builder flushRW = new FlushFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();
    private final KafkaDataExFW.Builder kafkaDataExRW = new KafkaDataExFW.Builder();
    private final KafkaFlushExFW.Builder kafkaFlushExRW = new KafkaFlushExFW.Builder();
    private final KafkaResetExFW.Builder kafkaResetExRW = new KafkaResetExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final OctetsFW valueFragmentRO = new OctetsFW();
    private final KafkaCacheEntryFW entryRO = new KafkaCacheEntryFW();


    private final int kafkaTypeId;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final Signaler signaler;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongSupplier supplyBudgetId;
    private final Function<String, KafkaCache> supplyCache;
    private final LongFunction<KafkaCacheRoute> supplyCacheRoute;
    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final LongToIntFunction supplyRemoteIndex;
    private final KafkaCacheCursorFactory cursorFactory;
    private final CRC32C crc32c;

    public KafkaCacheServerProduceFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        Signaler signaler,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        LongSupplier supplyBudgetId,
        ToIntFunction<String> supplyTypeId,
        Function<String, KafkaCache> supplyCache,
        LongFunction<KafkaCacheRoute> supplyCacheRoute,
        Long2ObjectHashMap<MessageConsumer> correlations,
        LongToIntFunction supplyRemoteIndex)
    {
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.router = router;
        this.writeBuffer = writeBuffer;
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.signaler = signaler;
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.supplyTraceId = supplyTraceId;
        this.supplyBudgetId = supplyBudgetId;
        this.supplyCache = supplyCache;
        this.supplyCacheRoute = supplyCacheRoute;
        this.correlations = correlations;
        this.cursorFactory = new KafkaCacheCursorFactory(writeBuffer);
        this.supplyRemoteIndex = supplyRemoteIndex;
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
        final long routeId = begin.routeId();
        final long initialId = begin.streamId();
        final long affinity = begin.affinity();
        final long authorization = begin.authorization();

        assert (initialId & 0x0000_0000_0000_0001L) != 0L;

        final OctetsFW extension = begin.extension();
        final ExtensionFW beginEx = extension.get(extensionRO::wrap);
        assert beginEx != null && beginEx.typeId() == kafkaTypeId;
        final KafkaBeginExFW kafkaBeginEx = extension.get(kafkaBeginExRO::wrap);
        final KafkaProduceBeginExFW kafkaProduceBeginEx = kafkaBeginEx.produce();

        final String16FW beginTopic = kafkaProduceBeginEx.topic();
        final int partitionId = kafkaProduceBeginEx.partition().partitionId();
        final int remoteIndex = supplyRemoteIndex.applyAsInt(initialId);

        MessageConsumer newStream = null;

        final MessagePredicate filter = (t, b, i, l) ->
        {
            final RouteFW route = wrapRoute.apply(t, b, i, l);
            final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
            final String16FW routeTopic = routeEx != null ? routeEx.topic() : null;
            return !route.localAddress().equals(route.remoteAddress()) &&
                    (beginTopic != null && (routeTopic == null || routeTopic.equals(beginTopic)));
        };

        final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
        if (route != null)
        {
            final String topicName = beginTopic.asString();
            final long resolvedId = route.correlationId();
            final KafkaCacheRoute cacheRoute = supplyCacheRoute.apply(resolvedId);
            final long partitionKey = cacheRoute.topicPartitionKey(topicName, partitionId);
            KafkaCacheServerProduceFan fan = cacheRoute.serverProduceFansByTopicPartition.get(partitionKey);
            if (fan == null)
            {
                final KafkaCacheServerProduceFan newFan = new KafkaCacheServerProduceFan(resolvedId, authorization,
                        affinity, partitionId, topicName);

                cacheRoute.serverProduceFansByTopicPartition.put(partitionKey, newFan);
                fan = newFan;
            }

            final int leaderId = cacheRoute.leadersByPartitionId.get(partitionId);
            final String cacheName = route.localAddress().asString();
            final KafkaCache cache = supplyCache.apply(cacheName);
            final KafkaCacheTopic topic = cache.supplyTopic(topicName);
            final KafkaCachePartition partition = topic.supplyProducePartition(partitionId, remoteIndex);

            newStream = new KafkaCacheServerProduceStream(
                    fan,
                    sender,
                    routeId,
                    initialId,
                    leaderId,
                    authorization,
                    partition)::onServerMessage;
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
        Consumer<OctetsFW.Builder> extension)
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
                .extension(extension)
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
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

    final class KafkaCacheServerProduceFan
    {
        private final long routeId;
        private final long authorization;
        private final long creditorId;
        private final int partitionId;
        private final String partionTopic;
        private final List<KafkaCacheServerProduceStream> members;

        private long leaderId;
        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private long initialBudgetId;
        private int initialBudget;
        private int initialPadding;
        private int initialFlags;

        private long reconnectAt = NO_CANCEL_ID;
        private int reconnectAttempt;
        private int memberIndex;

        private KafkaCacheServerProduceFan(
            long routeId,
            long authorization,
            long leaderId,
            int partitionId,
            String partionTopic)
        {
            this.routeId = routeId;
            this.authorization = authorization;
            this.partitionId = partitionId;
            this.partionTopic = partionTopic;
            this.members = new ArrayList<>();
            this.leaderId = leaderId;
            this.creditorId = supplyBudgetId.getAsLong();
        }

        private void onServerFanMemberOpening(
            long traceId,
            KafkaCacheServerProduceStream member)
        {
            if (member.leaderId != leaderId)
            {
                correlations.remove(replyId);
                doServerFanInitialAbortIfNecessary(traceId);
                doServerFanReplyResetIfNecessary(traceId);
                leaderId = member.leaderId;

                members.forEach(m -> m.cleanupServer(traceId, ERROR_NOT_LEADER_FOR_PARTITION));
                members.clear();
            }

            members.add(member);

            assert !members.isEmpty();

            doServerFanInitialBeginIfNecessary(traceId);

            if (KafkaState.initialOpened(state))
            {
                member.doServerInitialWindowIfNecessary(traceId);
            }

            if (KafkaState.replyOpened(state))
            {
                member.doServerReplyBeginIfNecessary(traceId);
            }
        }

        private void onServerFanMemberClosed(
            long traceId,
            KafkaCacheServerProduceStream member)
        {
            members.remove(member);

            member.cursor.close();

            if (members.isEmpty())
            {
                if (reconnectAt != NO_CANCEL_ID)
                {
                    signaler.cancel(reconnectAt);
                    this.reconnectAt = NO_CANCEL_ID;
                }

                doServerFanInitialEndIfNecessary(traceId);

                correlations.remove(replyId);
                state = KafkaState.closedReply(state);
            }
        }

        private void doServerFanInitialBeginIfNecessary(
            long traceId)
        {
            if (KafkaState.closed(state))
            {
                state = 0;
            }

            if (!KafkaState.initialOpening(state))
            {
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("%d %s PRODUCE connect, affinity %d\n", partitionId, partionTopic, leaderId);
                }

                doServerFanInitialBegin(traceId);
            }
        }

        private void doServerFanInitialBegin(
            long traceId)
        {
            assert state == 0;

            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onServerFanMessage);
            router.setThrottle(initialId, this::onServerFanMessage);
            doBegin(receiver, routeId, initialId, traceId, authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .produce(pr -> pr.transaction(TRANSACTION_NONE)
                                       .topic(partionTopic)
                                       .partition(part -> part.partitionId(partitionId).partitionOffset(DEFAULT_LATEST_OFFSET)))
                        .build()
                        .sizeof()));
            state = KafkaState.openingInitial(state);
        }

        private void doServerFanInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doServerFanInitialEnd(traceId);
            }
        }

        private void doServerFanInitialDataIfNecessary(
            long traceId)
        {
            final int membersCount = members.size();

            for (int membersNotProgressing = 0; membersNotProgressing < membersCount; membersNotProgressing++)
            {
                memberIndex = memberIndex >= membersCount ? 0 : memberIndex;

                final KafkaCacheServerProduceStream member = members.get(memberIndex);
                final long cursorOffset = member.cursor.offset;
                if (cursorOffset <= member.partitionOffset)
                {
                    member.doProduceInitialData(traceId);
                }

                if (member.messageOffset != 0)
                {
                    break;
                }

                memberIndex++;

                if (member.cursor.offset > cursorOffset)
                {
                    membersNotProgressing = 0;
                    continue;
                }
            }
        }

        private void doServerFanInitialData(
            long traceId,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload,
            Consumer<OctetsFW.Builder> extension)
        {
            if (KafkaConfiguration.DEBUG_PRODUCE)
            {
                System.out.format("[%d] [%d] [%d] kafka cache server fan [%d %s] %d - %d => %d\n",
                        currentTimeMillis(), currentThread().getId(),
                        initialId, partitionId, partionTopic, initialBudget, reserved, initialBudget - reserved);
            }

            assert (flags & FLAG_INIT) != (initialFlags & FLAG_INIT);

            initialFlags |= flags;

            if ((initialFlags & FLAG_FIN) != 0)
            {
                initialFlags = 0;
            }

            initialBudget -= reserved;
            assert initialBudget >= 0;

            doData(receiver, routeId, initialId, traceId, authorization, flags, initialBudgetId, reserved, payload, extension);
        }

        private void doServerFanInitialEnd(
            long traceId)
        {
            doEnd(receiver, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);

            onServerFanInitialClosed();
        }

        private void doServerFanInitialAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doServerFanInitialAbort(traceId);
            }
        }

        private void doServerFanInitialAbort(
            long traceId)
        {
            doAbort(receiver, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);

            onServerFanInitialClosed();
        }

        private void onServerFanInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final OctetsFW extension = reset.extension();

            onServerFanInitialClosed();

            doServerFanReplyResetIfNecessary(traceId);

            final KafkaResetExFW kafkaResetEx = extension.get(kafkaResetExRO::tryWrap);
            final int error = kafkaResetEx != null ? kafkaResetEx.error() : -1;

            if (KafkaConfiguration.DEBUG)
            {
                System.out.format("%d %s PRODUCE disconnect, error %d\n", partitionId, partionTopic, error);
            }

            members.forEach(s -> s.doServerInitialResetIfNecessary(traceId, extension));
        }

        private void onServerFanInitialWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            if (KafkaConfiguration.DEBUG_PRODUCE)
            {
                System.out.format("[%d] [%d] [%d] kafka cache server fan [%d %s] %d + %d => %d\n",
                        currentTimeMillis(), currentThread().getId(),
                        initialId, partitionId, partionTopic, initialBudget, credit, initialBudget + credit);
            }

            if (ReaktorConfiguration.DEBUG_BUDGETS)
            {
                System.out.format("[%d] [0x%016x] [0x%016x] cache server credit %d @ %d => %d\n",
                        System.nanoTime(), traceId, budgetId, credit, initialBudget, initialBudget + credit);
            }

            assert budgetId == 0L;

            initialBudgetId = budgetId;
            initialBudget += credit;
            initialPadding = padding;

            if (!KafkaState.initialOpened(state))
            {
                onServerFanInitialOpened();

                members.forEach(s -> s.doServerInitialWindowIfNecessary(traceId));
            }

            doServerFanInitialDataIfNecessary(traceId);
        }

        private void onServerFanInitialOpened()
        {
            assert !KafkaState.initialOpened(state);
            state = KafkaState.openedInitial(state);

            this.reconnectAttempt = 0;
        }

        private void onServerFanInitialClosed()
        {
            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            initialFlags = 0;
            initialBudget = 0;
            initialPadding = 0;
        }

        private void onServerFanMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onServerFanReplyBegin(begin);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onServerFanReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onServerFanReplyAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onServerFanInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onServerFanInitialWindow(window);
                break;
            default:
                break;
            }
        }

        private void onServerFanReplyBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final OctetsFW extension = begin.extension();
            final ExtensionFW beginEx = extension.get(extensionRO::tryWrap);
            assert beginEx != null && beginEx.typeId() == kafkaTypeId;
            final KafkaBeginExFW kafkaBeginEx = extension.get(kafkaBeginExRO::wrap);
            assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_PRODUCE;
            final KafkaProduceBeginExFW kafkaProduceBeginEx = kafkaBeginEx.produce();
            final int partitionId = kafkaProduceBeginEx.partition().partitionId();

            state = KafkaState.openedReply(state);

            assert partitionId == this.partitionId;

            members.forEach(s -> s.doServerReplyBeginIfNecessary(traceId));

            doServerFanReplyWindow(traceId, 0);
        }

        private void onServerFanReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            doServerFanInitialEndIfNecessary(traceId);

            if (KafkaConfiguration.DEBUG)
            {
                System.out.format("%d %s PRODUCE disconnect\n", partitionId, partionTopic);
            }

            members.forEach(s -> s.doServerReplyEndIfNecessary(traceId));
        }

        private void onServerFanReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            doServerFanInitialAbortIfNecessary(traceId);

            if (KafkaConfiguration.DEBUG)
            {
                System.out.format("%d %s PRODUCE disconnect\n", partitionId, partionTopic);
            }

            members.forEach(s -> s.doServerReplyAbortIfNecessary(traceId));
        }

        private void onServerFanSignal(
            int signalId)
        {
            assert signalId == SIGNAL_RECONNECT;

            this.reconnectAt = NO_CANCEL_ID;

            final long traceId = supplyTraceId.getAsLong();

            doServerFanInitialBeginIfNecessary(traceId);
        }

        private void doServerFanReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doServerFanReplyReset(traceId);
            }
        }

        private void doServerFanReplyReset(
            long traceId)
        {
            correlations.remove(replyId);

            state = KafkaState.closedReply(state);

            doReset(receiver, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doServerFanReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            doWindow(receiver, routeId, replyId, traceId, authorization, 0L, credit, 0);
        }
    }

    private final class KafkaCacheServerProduceStream
    {

        private final KafkaCachePartition partition;
        private final KafkaCacheCursor cursor;
        private final KafkaCacheServerProduceFan fan;
        private final MessageConsumer sender;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long leaderId;
        private final long authorization;

        private int state;

        private int initialBudget;
        private long partitionOffset = DEFAULT_LATEST_OFFSET;
        private int messageOffset;

        KafkaCacheServerProduceStream(
            KafkaCacheServerProduceFan fan,
            MessageConsumer sender,
            long routeId,
            long initialId,
            long leaderId,
            long authorization,
            KafkaCachePartition partition)
        {
            this.partition = partition;
            this.cursor = cursorFactory.newCursor(cursorFactory.asCondition(EMPTY_FILTER), KafkaDeltaType.NONE);
            this.fan = fan;
            this.sender = sender;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.leaderId = leaderId;
            this.authorization = authorization;
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
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onServerInitialFlush(flush);
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

                fan.onServerFanMemberOpening(traceId, this);
            }

            final OctetsFW extension = begin.extension();
            final ExtensionFW beginEx = extension.get(extensionRO::tryWrap);
            assert beginEx != null && beginEx.typeId() == kafkaTypeId;
            final KafkaBeginExFW kafkaBeginEx = extension.get(kafkaBeginExRO::wrap);
            assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_PRODUCE;
            final KafkaProduceBeginExFW kafkaProduceBeginEx = kafkaBeginEx.produce();
            final long partitionOffset = kafkaProduceBeginEx.partition().partitionOffset() + 1;

            KafkaCachePartition.Node segmentNode = partition.seekNotBefore(partitionOffset);

            if (segmentNode.sentinel())
            {
                segmentNode = segmentNode.next();
            }
            cursor.init(segmentNode, partitionOffset, DEFAULT_LATEST_OFFSET);
        }


        private void onServerInitialFlush(
            FlushFW flush)
        {
            final long traceId = flush.traceId();
            final int reserved = flush.reserved();
            final OctetsFW extension = flush.extension();
            final KafkaFlushExFW kafkaFlushEx = extension.get(kafkaFlushExRO::wrap);
            final KafkaProduceFlushExFW kafkaProduceFlushEx = kafkaFlushEx.produce();
            final KafkaOffsetFW partition = kafkaProduceFlushEx.partition();
            final long partitionOffset = partition.partitionOffset();

            assert partitionOffset >= this.partitionOffset;
            this.partitionOffset = partitionOffset;

            // defer reply window credit until next tick
            assert reserved == KafkaCacheServerFetchFactory.SIZE_OF_FLUSH_WITH_EXTENSION;

            fan.doServerFanInitialDataIfNecessary(traceId);
        }

        private void onServerInitialEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedInitial(state);

            if (messageOffset == 0)
            {
                fan.onServerFanMemberClosed(traceId, this);

                doServerReplyEndIfNecessary(traceId);
            }
        }

        private void onServerInitialAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedInitial(state);

            if (messageOffset == 0)
            {
                fan.onServerFanMemberClosed(traceId, this);

                doServerReplyAbortIfNecessary(traceId);
            }
        }

        private void doProduceInitialData(
            long traceId)
        {
            if (cursor.offset <= partitionOffset)
            {
                final KafkaCacheEntryFW nextEntry = cursor.next(entryRO);

                if (nextEntry != null)
                {
                    final long partitionOffset = nextEntry.offset$();
                    final long timestamp = nextEntry.timestamp();
                    final int sequence = nextEntry.sequence();
                    final KafkaKeyFW key = nextEntry.key();
                    final ArrayFW<KafkaHeaderFW> headers = nextEntry.headers();
                    final OctetsFW value = nextEntry.value();
                    final int remaining = value != null ? value.sizeof() - messageOffset : 0;
                    assert remaining >= 0;
                    final int initialPadding = fan.initialPadding;
                    final int initialBudget = fan.initialBudget;
                    final int reserved = Math.min(remaining + initialPadding, initialBudget);

                    assert partitionOffset >= cursor.offset : String.format("%d >= %d", partitionOffset, cursor.offset);

                    if (reserved >= initialPadding)
                    {
                        final int length = Math.min(reserved - initialPadding, remaining);
                        assert length >= 0 : String.format("%d >= 0", length);

                        final int deferred = remaining - length;
                        assert deferred >= 0 : String.format("%d >= 0", deferred);

                        int flags = 0x00;
                        if (messageOffset == 0)
                        {
                            flags |= FLAG_INIT;
                        }
                        if (length == remaining)
                        {
                            flags |= FLAG_FIN;
                        }

                        OctetsFW fragment = value;
                        if (flags != (FLAG_INIT | FLAG_FIN))
                        {
                            final int fragmentOffset = value.offset() + messageOffset;
                            final int fragmentLimit = fragmentOffset + length;
                            fragment = valueFragmentRO.wrap(value.buffer(), fragmentOffset, fragmentLimit);
                        }

                        long checksum = 0;
                        if ((flags & FLAG_INIT) == FLAG_INIT && value != null)
                        {
                            final ByteBuffer buffer = value.value().byteBuffer();
                            buffer.limit(value.limit());
                            buffer.position(value.offset());
                            crc32c.reset();
                            crc32c.update(buffer);
                            checksum = crc32c.getValue();
                        }

                        switch (flags)
                        {
                        case FLAG_INIT | FLAG_FIN:
                            doServerInitialDataFull(traceId, timestamp, sequence, checksum, key, headers,
                                fragment, reserved, flags);
                            break;
                        case FLAG_INIT:
                            doServerInitialDataInit(traceId, deferred, timestamp, sequence, checksum, key,
                                headers, fragment, reserved, flags);
                            break;
                        case FLAG_NONE:
                            doServerInitialDataNone(traceId, fragment, reserved, length, flags);
                            break;
                        case FLAG_FIN:
                            doServerInitialDataFin(traceId, headers, fragment, reserved, flags);
                            break;
                        }

                        if ((flags & FLAG_FIN) == 0x00)
                        {
                            this.messageOffset += length;
                        }
                        else
                        {
                            this.messageOffset = 0;

                            if (KafkaState.initialClosed(state))
                            {
                                fan.onServerFanMemberClosed(traceId, this);

                                doServerReplyEndIfNecessary(traceId);
                            }
                            else
                            {
                                cursor.advance(partitionOffset + 1);
                            }

                            doFlushServerReply(traceId);
                        }
                    }
                }
            }

        }

        private void doServerInitialDataFull(
            long traceId,
            long timestamp,
            int sequence,
            long checksum,
            KafkaKeyFW key,
            ArrayFW<KafkaHeaderFW> headers,
            OctetsFW value,
            int reserved,
            int flags)
        {
            fan.doServerFanInitialData(traceId, flags, 0L, reserved, value,
                ex -> ex.set((b, o, l) -> kafkaDataExRW.wrap(b, o, l)
                           .typeId(kafkaTypeId)
                           .produce(f -> f.timestamp(timestamp)
                                        .sequence(sequence)
                                        .crc32c(checksum)
                                        .key(k -> k.length(key.length()).value(key.value()))
                                        .headers(hs -> headers.forEach(h -> hs.item(i -> i.nameLen(h.nameLen())
                                                                                          .name(h.name())
                                                                                          .valueLen(h.valueLen())
                                                                                          .value(h.value())))))
                           .build()
                           .sizeof()));
        }

        private void doServerInitialDataInit(
            long traceId,
            int deferred,
            long timestamp,
            int sequence,
            long checksum,
            KafkaKeyFW key,
            ArrayFW<KafkaHeaderFW> headers,
            OctetsFW value,
            int reserved,
            int flags)
        {
            fan.doServerFanInitialData(traceId, flags, 0L, reserved, value,
                ex -> ex.set((b, o, l) -> kafkaDataExRW.wrap(b, o, l)
                           .typeId(kafkaTypeId)
                           .produce(f -> f.deferred(deferred)
                                          .timestamp(timestamp)
                                          .sequence(sequence)
                                          .crc32c(checksum)
                                          .key(k -> k.length(key.length()).value(key.value()))
                                          .headers(hs -> headers.forEach(h -> hs.item(i -> i.nameLen(h.nameLen())
                                                                                            .name(h.name())
                                                                                            .valueLen(h.valueLen())
                                                                                            .value(h.value())))))
                           .build()
                           .sizeof()));
        }

        private void doServerInitialDataNone(
            long traceId,
            OctetsFW fragment,
            int reserved,
            int length,
            int flags)
        {
            fan.doServerFanInitialData(traceId, flags, 0L, reserved, fragment, EMPTY_EXTENSION);
        }

        private void doServerInitialDataFin(
            long traceId,
            ArrayFW<KafkaHeaderFW> headers,
            OctetsFW fragment,
            int reserved,
            int flags)
        {
            fan.doServerFanInitialData(traceId, flags, 0L, reserved, fragment,
                ex -> ex.set((b, o, l) -> kafkaDataExRW.wrap(b, o, l)
                           .typeId(kafkaTypeId)
                           .produce(f -> f.headers(hs -> headers.forEach(h -> hs.item(i -> i.nameLen(h.nameLen())
                                                                                            .name(h.name())
                                                                                            .valueLen(h.valueLen())
                                                                                            .value(h.value())))))
                           .build()
                           .sizeof()));
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
            long traceId)
        {
            final int credit = Math.max(fan.initialBudget - initialBudget, 0);

            if (!KafkaState.initialOpened(state) || credit > 0)
            {
                doServerInitialWindow(traceId, credit);
            }
        }

        private void doServerInitialWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedInitial(state);

            if (KafkaConfiguration.DEBUG_PRODUCE)
            {
                System.out.format("[%d] [%d] [%d] kafka cache server [%s] %d + %d => %d\n",
                        currentTimeMillis(), currentThread().getId(),
                        initialId, partition, initialBudget, credit, initialBudget + credit);
            }

            initialBudget += credit;

            doWindow(sender, routeId, initialId, traceId, authorization,
                    fan.creditorId, credit, fan.initialPadding);
        }

        private void doFlushServerReply(
            long traceId)
        {
            doFlush(sender, routeId, replyId, traceId, authorization, 0L, SIZE_OF_FLUSH_WITH_EXTENSION,
                ex -> ex.set((b, o, l) -> kafkaFlushExRW.wrap(b, o, l)
                                                        .typeId(kafkaTypeId)
                                                        .produce(f -> f.partition(p -> p.partitionId(partition.id())
                                                                                        .partitionOffset(partitionOffset)))
                                                        .build()
                                                        .sizeof()));
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

            router.setThrottle(replyId, this::onServerMessage);
            doBegin(sender, routeId, replyId, traceId, authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .produce(p -> p.transaction(TRANSACTION_NONE)
                                       .topic(fan.partionTopic)
                                       .partition(par -> par.partitionId(fan.partitionId).partitionOffset(
                                           DEFAULT_LATEST_OFFSET)))
                        .build()
                        .sizeof()));
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
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            assert budgetId == 0L;
            assert credit == 0;
            assert padding == 0;

            state = KafkaState.openedReply(state);
        }

        private void onServerReplyReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedReply(state);

            if (messageOffset  == 0)
            {
                fan.onServerFanMemberClosed(traceId, this);
            }

            doServerInitialResetIfNecessary(traceId, EMPTY_OCTETS);
        }

        private boolean isLive()
        {
            return cursor.offset == partitionOffset;
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
