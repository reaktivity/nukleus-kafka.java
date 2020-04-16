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
import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_CREDITOR_INDEX;
import static org.reaktivity.nukleus.concurrent.Signaler.NO_CANCEL_ID;

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
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.budget.KafkaCacheServerBudget;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartition;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheTopic;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ExtensionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaProduceBeginExFW;
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

    private static final DirectBuffer EMPTY_BUFFER = new UnsafeBuffer();
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(EMPTY_BUFFER, 0, 0);
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};

    private static final int SIGNAL_RECONNECT = 1;

    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();
    private final KafkaResetExFW kafkaResetExRO = new KafkaResetExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final int kafkaTypeId;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final Signaler signaler;
    private final BudgetCreditor creditor;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongSupplier supplyBudgetId;
    private final Function<String, KafkaCache> supplyCache;
    private final LongFunction<KafkaCacheRoute> supplyCacheRoute;
    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final int reconnectDelay;

    public KafkaCacheServerProduceFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        Signaler signaler,
        BudgetCreditor creditor,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        LongSupplier supplyBudgetId,
        ToIntFunction<String> supplyTypeId,
        Function<String, KafkaCache> supplyCache,
        LongFunction<KafkaCacheRoute> supplyCacheRoute,
        Long2ObjectHashMap<MessageConsumer> correlations)
    {
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.router = router;
        this.writeBuffer = writeBuffer;
        this.signaler = signaler;
        this.creditor = creditor;
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.supplyTraceId = supplyTraceId;
        this.supplyBudgetId = supplyBudgetId;
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
        final KafkaProduceBeginExFW kafkaProduceBeginEx = kafkaBeginEx.produce();

        final String16FW beginTopic = kafkaProduceBeginEx.topic();
        final int partitionId = kafkaProduceBeginEx.partitionId();

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
            final long topicKey = cacheRoute.topicKey(topicName);
            final long partitionKey = cacheRoute.topicPartitionKey(topicName, partitionId);
            KafkaCacheServerProduceFanout fanout = cacheRoute.serverProduceFanoutsByTopicPartition.get(partitionKey);
            if (fanout == null)
            {
                KafkaCacheServerBudget budget = cacheRoute.serverBudgetsByTopic.get(topicKey);
                if (budget == null)
                {
                    budget = new KafkaCacheServerBudget(creditor, supplyBudgetId.getAsLong());
                    cacheRoute.serverBudgetsByTopic.put(topicKey, budget);
                }

                final String cacheName = route.localAddress().asString();
                final KafkaCache cache = supplyCache.apply(cacheName);
                final KafkaCacheTopic topic = cache.supplyTopic(topicName);
                final KafkaCachePartition partition = topic.supplyPartition(partitionId);
                final KafkaCacheServerProduceFanout newFanout = new KafkaCacheServerProduceFanout(resolvedId, authorization,
                        affinity, budget, partition);

                // TODO: fragmented cache server fan-in across cores
                cacheRoute.serverProduceFanoutsByTopicPartition.put(partitionKey, newFanout);
                fanout = newFanout;
            }

            if (fanout != null)
            {
                assert fanout.affinity == affinity || fanout.state == 0 || KafkaState.closed(fanout.state) :
                        String.format("%d == %d || %d == 0 || KafkaState.closed(0x%08x) // [%016x]",
                                fanout.affinity, affinity, fanout.state, fanout.state, fanout.initialId);

                fanout.affinity = affinity;

                newStream = new KafkaCacheServerProduceStream(
                        fanout,
                        sender,
                        routeId,
                        initialId,
                        affinity,
                        authorization)::onServerMessage;
            }
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
        OctetsFW extension)
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

    final class KafkaCacheServerProduceFanout
    {
        private final long routeId;
        private final long authorization;
        private final long creditorId;
        private final KafkaCacheServerBudget budget;
        private final KafkaCachePartition partition;
        private final List<KafkaCacheServerProduceStream> members;

        private long affinity;
        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private int initialBudget;
        private int initialPadding;

        private long creditorIndex = NO_CREDITOR_INDEX;

        private long reconnectAt = NO_CANCEL_ID;
        private int reconnectAttempt;

        private KafkaCacheServerProduceFanout(
            long routeId,
            long authorization,
            long affinity,
            KafkaCacheServerBudget budget,
            KafkaCachePartition partition)
        {
            this.routeId = routeId;
            this.authorization = authorization;
            this.partition = partition;
            this.members = new ArrayList<>();
            this.affinity = affinity;
            this.creditorId = supplyBudgetId.getAsLong();
            this.budget = budget;
        }

        private void onServerFanoutMemberOpening(
            long traceId,
            KafkaCacheServerProduceStream member)
        {
            members.add(member);

            assert !members.isEmpty();

            doServerFanoutInitialBeginIfNecessary(traceId);

            if (KafkaState.initialOpened(state))
            {
                member.doServerInitialWindowIfNecessary(traceId);
            }

            if (KafkaState.replyOpened(state))
            {
                member.doServerReplyBeginIfNecessary(traceId);
            }
        }

        private void onServerFanoutMemberClosed(
            long traceId,
            KafkaCacheServerProduceStream member)
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
                if (KafkaConfiguration.DEBUG)
                {
                    System.out.format("%s PRODUCE connect, affinity %d\n", partition, affinity);
                }

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

            correlations.put(replyId, this::onServerFanoutMessage);
            router.setThrottle(initialId, this::onServerFanoutMessage);
            doBegin(receiver, routeId, initialId, traceId, authorization, affinity,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .produce(p -> p.transaction(TRANSACTION_NONE)
                                       .topic(partition.topic())
                                       .partitionId(partition.id()))
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

        private void doServerFanoutInitialData(
            long traceId,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload,
            OctetsFW extension)
        {
            initialBudget -= reserved;
            assert initialBudget >= 0;

            budget.credit(traceId, creditorIndex, -reserved);

            doData(receiver, routeId, initialId, traceId, authorization, flags, budgetId, reserved, payload, extension);
        }

        private void doServerFanoutInitialEnd(
            long traceId)
        {
            doEnd(receiver, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);

            state = KafkaState.closedInitial(state);

            cleanupCreditorIfNecessary();
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

            cleanupCreditorIfNecessary();
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
            assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_PRODUCE;
            final KafkaProduceBeginExFW kafkaProduceBeginEx = kafkaBeginEx.produce();
            final int partitionId = kafkaProduceBeginEx.partitionId();

            state = KafkaState.openedReply(state);

            assert partitionId == partition.id();

            members.forEach(s -> s.doServerReplyBeginIfNecessary(traceId));

            doServerFanoutReplyWindow(traceId, 0);
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
                    System.out.format("%s PRODUCE reconnect in %ds\n", partition, reconnectDelay);
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
                    System.out.format("%s PRODUCE disconnect\n", partition);
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
                    System.out.format("%s PRODUCE reconnect in %ds\n", partition, reconnectDelay);
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
                    System.out.format("%s PRODUCE disconnect\n", partition);
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
                    System.out.format("%s PRODUCE reconnect in %ds, error %d\n", partition, reconnectDelay, error);
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
                    System.out.format("%s PRODUCE disconnect, error %d\n", partition, error);
                }

                members.forEach(s -> s.doServerInitialResetIfNecessary(traceId, extension));

                cleanupCreditorIfNecessary();
            }
        }

        private void onServerFanoutInitialWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            if (ReaktorConfiguration.DEBUG_BUDGETS)
            {
                System.out.format("[%d] [0x%016x] [0x%016x] cache server credit %d @ %d => %d\n",
                        System.nanoTime(), traceId, budgetId, credit, initialBudget, initialBudget + credit);
            }

            assert budgetId == 0L;
            initialBudget += credit;
            initialPadding = padding;

            if (!KafkaState.initialOpened(state))
            {
                this.reconnectAttempt = 0;

                state = KafkaState.openedInitial(state);

                if (creditorIndex == NO_CREDITOR_INDEX)
                {
                    creditorIndex = budget.acquire(creditorId);
                }
            }

            budget.credit(traceId, creditorIndex, credit);

            members.forEach(s -> s.doServerInitialWindowIfNecessary(traceId));
        }

        private void onServerFanoutSignal(
            int signalId)
        {
            assert signalId == SIGNAL_RECONNECT;

            this.reconnectAt = NO_CANCEL_ID;

            final long traceId = supplyTraceId.getAsLong();

            doServerFanoutInitialBeginIfNecessary(traceId);
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

        private void cleanupCreditorIfNecessary()
        {
            if (creditorIndex != NO_CREDITOR_INDEX)
            {
                budget.release(creditorIndex);
                creditorIndex = NO_CREDITOR_INDEX;
            }
        }
    }

    private final class KafkaCacheServerProduceStream
    {
        private final KafkaCacheServerProduceFanout group;
        private final MessageConsumer sender;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long authorization;

        private int state;

        private int initialBudget;

        KafkaCacheServerProduceStream(
            KafkaCacheServerProduceFanout group,
            MessageConsumer sender,
            long routeId,
            long initialId,
            long affinity,
            long authorization)
        {
            this.group = group;
            this.sender = sender;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.affinity = affinity;
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

            state = KafkaState.openingInitial(state);

            group.onServerFanoutMemberOpening(traceId, this);
        }

        private void onServerInitialData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            initialBudget -= reserved;

            if (initialBudget < 0)
            {
                doServerInitialResetIfNecessary(traceId, EMPTY_OCTETS);
                doServerReplyAbortIfNecessary(traceId);
                group.onServerFanoutMemberClosed(traceId, this);
            }
            else
            {
                group.doServerFanoutInitialData(traceId, flags, budgetId, reserved, payload, extension);
            }
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
            long traceId)
        {
            final int credit = Math.max(group.initialBudget - initialBudget, 0);

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

            initialBudget += credit;

            doWindow(sender, routeId, initialId, traceId, authorization,
                    group.budget.sharedBudgetId, credit, group.initialPadding);
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
            doBegin(sender, routeId, replyId, traceId, authorization, affinity,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .produce(p -> p.transaction(TRANSACTION_NONE)
                                       .topic(group.partition.topic())
                                       .partitionId(group.partition.id()))
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

            group.onServerFanoutMemberClosed(traceId, this);

            doServerInitialResetIfNecessary(traceId, EMPTY_OCTETS);
        }
    }
}
