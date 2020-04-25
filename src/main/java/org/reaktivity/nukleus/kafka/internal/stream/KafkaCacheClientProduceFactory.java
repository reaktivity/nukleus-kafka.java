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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaCacheClientProduceFactory implements StreamFactory
{
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(), 0, 0);
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};

    private static final String TRANSACTION_NONE = null;

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
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final Function<String, KafkaCache> supplyCache;
    private final LongFunction<KafkaCacheRoute> supplyCacheRoute;
    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final boolean cacheServerEnforcesInitialBudget;

    public KafkaCacheClientProduceFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
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
        this.writeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.supplyCache = supplyCache;
        this.supplyCacheRoute = supplyCacheRoute;
        this.correlations = correlations;
        this.cacheServerEnforcesInitialBudget = config.cacheServerEnforcesInitialBudget();
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
        final ExtensionFW beginEx = extension.get(extensionRO::tryWrap);
        assert beginEx != null && beginEx.typeId() == kafkaTypeId;
        final KafkaBeginExFW kafkaBeginEx = extension.get(kafkaBeginExRO::wrap);
        assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_PRODUCE;
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
            final long resolvedId = route.correlationId();
            final String topicName = beginTopic.asString();
            final KafkaCacheRoute cacheRoute = supplyCacheRoute.apply(resolvedId);
            final long partitionKey = cacheRoute.topicPartitionKey(topicName, partitionId);

            KafkaCacheClientProduceFanout fanout = cacheRoute.clientProduceFanoutsByTopicPartition.get(partitionKey);
            if (fanout == null)
            {
                final String cacheName = route.remoteAddress().asString();
                final KafkaCache cache = supplyCache.apply(cacheName);
                final KafkaCacheTopic topic = cache.supplyTopic(topicName);
                final KafkaCachePartition partition = topic.supplyPartition(partitionId);
                final KafkaCacheClientProduceFanout newFanout =
                        new KafkaCacheClientProduceFanout(resolvedId, authorization, affinity, partition);

                cacheRoute.clientProduceFanoutsByTopicPartition.put(partitionKey, newFanout);
                fanout = newFanout;
            }

            if (fanout != null)
            {
                assert fanout.affinity == affinity || fanout.state == 0 || KafkaState.closed(fanout.state) :
                        String.format("%d == %d || %d == 0 || KafkaState.closed(0x%08x)",
                                fanout.affinity, affinity, fanout.state, fanout.state);
                fanout.affinity = affinity;

                newStream = new KafkaCacheClientProduceStream(
                        fanout,
                        sender,
                        routeId,
                        initialId,
                        affinity,
                        authorization)::onClientMessage;
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

    final class KafkaCacheClientProduceFanout
    {
        private final long routeId;
        private final long authorization;
        private final KafkaCachePartition partition;
        private final List<KafkaCacheClientProduceStream> members;

        private long affinity;
        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private long initialBudgetId;
        private int initialBudget;
        private int initialPadding;

        private KafkaCacheClientProduceFanout(
            long routeId,
            long authorization,
            long affinity,
            KafkaCachePartition partition)
        {
            this.routeId = routeId;
            this.authorization = authorization;
            this.partition = partition;
            this.members = new ArrayList<>();
            this.affinity = affinity;
        }

        private void onClientFanoutMemberOpening(
            long traceId,
            KafkaCacheClientProduceStream member)
        {
            members.add(member);

            assert !members.isEmpty();

            doClientFanoutInitialBeginIfNecessary(traceId);

            if (KafkaState.initialOpened(state))
            {
                member.doClientInitialWindowIfNecessary(traceId);
            }

            if (isFanoutReplyOpened())
            {
                member.doClientReplyBeginIfNecessary(traceId);
            }
        }

        private boolean isFanoutReplyOpened()
        {
            return KafkaState.replyOpened(state);
        }

        private void onClientFanoutMemberClosed(
            long traceId,
            KafkaCacheClientProduceStream member)
        {
            members.remove(member);

            if (members.isEmpty())
            {
                correlations.remove(replyId);
                doClientFanoutInitialAbortIfNecessary(traceId);
                doClientFanoutReplyResetIfNecessary(traceId);
            }
        }

        private void doClientFanoutInitialBeginIfNecessary(
            long traceId)
        {
            if (KafkaState.closed(state))
            {
                state = 0;
                initialBudgetId = 0L;
                initialBudget = 0;
                initialPadding = 0;
            }

            if (!KafkaState.initialOpening(state))
            {
                doClientFanoutInitialBegin(traceId);
            }
        }

        private void doClientFanoutInitialBegin(
            long traceId)
        {
            assert state == 0;

            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onClientFanoutMessage);
            router.setThrottle(initialId, this::onClientFanoutMessage);
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

        private void doClientFanoutInitialData(
            long traceId,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload,
            OctetsFW extension)
        {
            initialBudget -= reserved;
            assert cacheServerEnforcesInitialBudget || initialBudget >= 0;

            doData(receiver, routeId, initialId, traceId, authorization, flags, budgetId, reserved, payload, extension);
        }

        private void doClientFanoutInitialAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doClientFanoutInitialAbort(traceId);
            }
        }

        private void doClientFanoutInitialAbort(
            long traceId)
        {
            doAbort(receiver, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);

            state = KafkaState.closedInitial(state);
        }

        private void onClientFanoutMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onClientFanoutReplyBegin(begin);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onClientFanoutReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onClientFanoutReplyAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onClientFanoutInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onClientFanoutInitialWindow(window);
                break;
            default:
                break;
            }
        }

        private void onClientFanoutReplyBegin(
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

            assert partitionId == this.partition.id();

            members.forEach(s -> s.doClientReplyBeginIfNecessary(traceId));

            doClientFanoutReplyWindow(traceId, 0);
        }

        private void onClientFanoutReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            members.forEach(s -> s.doClientReplyEndIfNecessary(traceId));

            state = KafkaState.closedReply(state);
        }

        private void onClientFanoutReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            members.forEach(s -> s.doClientReplyAbortIfNecessary(traceId));

            state = KafkaState.closedReply(state);
        }

        private void onClientFanoutInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final OctetsFW extension = reset.extension();

            members.forEach(s -> s.doClientInitialResetIfNecessary(traceId, extension));

            state = KafkaState.closedInitial(state);

            doClientFanoutReplyResetIfNecessary(traceId);
        }

        private void onClientFanoutInitialWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            state = KafkaState.openedInitial(state);

            initialBudgetId = budgetId;
            initialBudget += credit;
            initialPadding = padding;

            members.forEach(s -> s.doClientInitialWindowIfNecessary(traceId));
        }

        private void doClientFanoutReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doClientFanoutReplyReset(traceId);
            }
        }

        private void doClientFanoutReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);

            doReset(receiver, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doClientFanoutReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            doWindow(receiver, routeId, replyId, traceId, authorization, 0L, credit, 0);
        }
    }

    private final class KafkaCacheClientProduceStream
    {
        private final KafkaCacheClientProduceFanout group;
        private final MessageConsumer sender;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long authorization;

        private int state;

        private int initialBudget;

        KafkaCacheClientProduceStream(
            KafkaCacheClientProduceFanout group,
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

        private void onClientMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onClientInitialBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onClientInitialData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onClientInitialEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onClientInitialAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onClientReplyWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onClientReplyReset(reset);
                break;
            default:
                break;
            }
        }

        private void onClientInitialBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();

            state = KafkaState.openingInitial(state);

            group.onClientFanoutMemberOpening(traceId, this);
        }

        private void onClientInitialData(
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
                doClientInitialResetIfNecessary(traceId, EMPTY_OCTETS);
                doClientReplyAbortIfNecessary(traceId);
                group.onClientFanoutMemberClosed(traceId, this);
            }
            else
            {
                group.doClientFanoutInitialData(traceId, flags, budgetId, reserved, payload, extension);
            }
        }

        private void onClientInitialEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedInitial(state);

            group.onClientFanoutMemberClosed(traceId, this);

            doClientReplyEndIfNecessary(traceId);
        }

        private void onClientInitialAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedInitial(state);

            group.onClientFanoutMemberClosed(traceId, this);

            doClientReplyAbortIfNecessary(traceId);
        }

        private void doClientInitialWindowIfNecessary(
            long traceId)
        {
            final int credit = Math.max(group.initialBudget - initialBudget, 0);

            if (!KafkaState.initialOpened(state) || credit > 0)
            {
                doClientInitialWindow(traceId, credit);
            }
        }

        private void doClientInitialWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedInitial(state);

            initialBudget += credit;

            doWindow(sender, routeId, initialId, traceId, authorization,
                    group.initialBudgetId, credit, group.initialPadding);
        }

        private void doClientInitialResetIfNecessary(
            long traceId,
            Flyweight extension)
        {
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doClientInitialReset(traceId, extension);
            }

            state = KafkaState.closedInitial(state);
        }

        private void doClientInitialReset(
            long traceId,
            Flyweight extension)
        {
            state = KafkaState.closedInitial(state);

            doReset(sender, routeId, initialId, traceId, authorization, extension);
        }

        private void doClientReplyBeginIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyOpening(state))
            {
                doClientReplyBegin(traceId);
            }
        }

        private void doClientReplyBegin(
            long traceId)
        {
            state = KafkaState.openingReply(state);

            router.setThrottle(replyId, this::onClientMessage);
            doBegin(sender, routeId, replyId, traceId, authorization, affinity,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .produce(p -> p.transaction(TRANSACTION_NONE)
                                       .topic(group.partition.topic())
                                       .partitionId(group.partition.id()))
                        .build()
                        .sizeof()));
        }

        private void doClientReplyEnd(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            doEnd(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doClientReplyAbort(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            doAbort(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doClientReplyEndIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doClientReplyEnd(traceId);
            }

            state = KafkaState.closedReply(state);
        }

        private void doClientReplyAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doClientReplyAbort(traceId);
            }

            state = KafkaState.closedReply(state);
        }

        private void onClientReplyWindow(
            WindowFW window)
        {
            state = KafkaState.openedReply(state);
        }

        private void onClientReplyReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedReply(state);

            group.onClientFanoutMemberClosed(traceId, this);

            doClientInitialResetIfNecessary(traceId, EMPTY_OCTETS);
        }
    }
}
