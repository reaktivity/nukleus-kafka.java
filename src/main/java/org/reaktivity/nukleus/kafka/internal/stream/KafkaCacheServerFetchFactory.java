/**
 * Copyright 2016-2019 The Reaktivity Project
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
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaCacheServerFetchFactory implements StreamFactory
{
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};

    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();
    private final FlushFW flushRO = new FlushFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();
    private final KafkaDataExFW kafkaDataExRO = new KafkaDataExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();
    private final KafkaDataExFW.Builder kafkaDataExRW = new KafkaDataExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final int kafkaTypeId;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final BufferPool bufferPool;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final Map<String, KafkaCacheServerFetchFanout> fanoutsByTopic;
    private final Long2ObjectHashMap<MessageConsumer> correlations;

    public KafkaCacheServerFetchFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        Long2ObjectHashMap<MessageConsumer> correlations)
    {
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.router = router;
        this.writeBuffer = writeBuffer;
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = bufferPool;
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.correlations = correlations;
        this.fanoutsByTopic = new TreeMap<>();
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

        assert (streamId & 0x0000_0000_0000_0001L) != 0L;

        final OctetsFW extension = begin.extension();
        final ExtensionFW beginEx = extensionRO.tryWrap(extension.buffer(), extension.offset(), extension.limit());
        final KafkaBeginExFW kafkaBeginEx = beginEx != null && beginEx.typeId() == kafkaTypeId ?
                kafkaBeginExRO.tryWrap(extension.buffer(), extension.offset(), extension.limit()) : null;

        assert kafkaBeginEx != null;
        assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_FETCH;
        final String16FW beginTopic = kafkaBeginEx.fetch().topic();
        final String topic = beginTopic != null ? beginTopic.asString() : null;

        KafkaCacheServerFetchFanout fanout = fanoutsByTopic.get(topic);
        if (fanout == null)
        {
            final long routeId = begin.routeId();
            final long authorization = begin.authorization();

            final MessagePredicate filter = (t, b, i, l) ->
            {
                final RouteFW route = wrapRoute.apply(t, b, i, l);
                final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
                final String16FW routeTopic = routeEx != null ? routeEx.topic() : null;
                return routeTopic == null || Objects.equals(routeTopic, beginTopic);
            };

            final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
            if (route != null)
            {
                final long resolvedId = route.correlationId();
                final KafkaCacheServerFetchFanout newFanout = new KafkaCacheServerFetchFanout(resolvedId, authorization, topic);

                fanoutsByTopic.put(topic, newFanout);
                fanout = newFanout;
            }
        }

        MessageConsumer newStream = null;
        if (fanout != null)
        {
            newStream = fanout.newMemberInitialStream(begin, sender);
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
        long budgetId,
        int reserved,
        OctetsFW payload,
        Flyweight extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload)
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
        long authorization)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .traceId(traceId)
               .authorization(authorization)
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private final class KafkaCacheServerFetchFanout
    {
        private final long routeId;
        private final long authorization;
        private final String topic;
        private final List<KafkaCacheServerFetchStream> members;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private KafkaCacheServerFetchFanout(
            long routeId,
            long authorization,
            String topic)
        {
            this.routeId = routeId;
            this.authorization = authorization;
            this.topic = topic;
            this.members = new ArrayList<>();
        }

        private MessageConsumer newMemberInitialStream(
            BeginFW begin,
            MessageConsumer application)
        {
            final long routeId = begin.routeId();
            final long initialId = begin.streamId();
            final long affinity = begin.affinity();
            final long authorization = begin.authorization();

            return new KafkaCacheServerFetchStream(
                    this,
                    application,
                    routeId,
                    initialId,
                    affinity,
                    authorization)::onInitial;
        }

        private void onMemberOpening(
            long traceId,
            KafkaCacheServerFetchStream member)
        {
            members.add(member);

            assert !members.isEmpty();

            doInitialBeginIfNecessary(traceId);

            if (KafkaState.initialOpened(state))
            {
                member.doInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }

            if (KafkaState.replyOpened(state))
            {
                member.doReplyBeginIfNecessary(traceId);
            }
        }

        private void onMemberOpened(
            long traceId,
            KafkaCacheServerFetchStream member)
        {
            if (KafkaState.replyOpened(state))
            {
                member.doReplyData(traceId);
            }
        }

        private void onMemberClosed(
            long traceId,
            KafkaCacheServerFetchStream member)
        {
            members.remove(member);

            if (members.isEmpty())
            {
                correlations.remove(replyId);
                doInitialEndIfNecessary(traceId);
            }
        }

        private void doInitialBeginIfNecessary(
            long traceId)
        {
            if (KafkaState.initialClosed(state) &&
                KafkaState.replyClosed(state))
            {
                state = 0;
            }

            if (!KafkaState.initialOpening(state))
            {
                doInitialBegin(traceId);
            }
        }

        private void doInitialBegin(
            long traceId)
        {
            assert state == 0;

            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onReply);
            router.setThrottle(initialId, this::onReply);
            doBegin(receiver, routeId, initialId, traceId, authorization, 0L,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .meta(m -> m.topic(topic))
                        .build()
                        .sizeof()));
            state = KafkaState.openingInitial(state);
        }

        private void doInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doInitialEnd(traceId);
            }
        }

        private void doInitialEnd(
            long traceId)
        {
            doEnd(receiver, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);

            state = KafkaState.closedInitial(state);
        }

        private void onReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onReplyBegin(begin);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onReplyAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onInitialWindow(window);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onInitialFlush(flush);
                break;
            default:
                break;
            }
        }

        private void onReplyBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            doReplyWindow(traceId, bufferPool.slotCapacity());

            state = KafkaState.openedReply(state);

            members.forEach(s -> s.doReplyBeginIfNecessary(traceId));
        }

        private void onInitialFlush(
            FlushFW flush)
        {
            final long traceId = flush.traceId();

            members.forEach(s -> s.doReplyData(traceId));
        }

        private void onReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            members.forEach(s -> s.doReplyEndIfNecessary(traceId));

            state = KafkaState.closedReply(state);
        }

        private void onReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            members.forEach(s -> s.doReplyAbortIfNecessary(traceId));

            state = KafkaState.closedReply(state);
        }

        private void onInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            members.forEach(s -> s.doInitialResetIfNecessary(traceId));

            state = KafkaState.closedInitial(state);

            doReplyResetIfNecessary(traceId);
        }

        private void onInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                members.forEach(s -> s.doInitialWindowIfNecessary(traceId, 0L, 0, 0));
            }
        }

        private void doReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doReplyReset(traceId);
            }
        }

        private void doReplyReset(
            long traceId)
        {
            doReset(receiver, routeId, replyId, traceId, authorization);

            state = KafkaState.closedReply(state);
        }

        private void doReplyWindow(
            long traceId,
            int credit)
        {
            doWindow(receiver, routeId, replyId, traceId, authorization, 0L, credit, 0);

            state = KafkaState.closedInitial(state);
        }
    }

    private final class KafkaCacheServerFetchStream
    {
        private final KafkaCacheServerFetchFanout group;
        private final MessageConsumer sender;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long authorization;

        private int state;

        private long replyBudgetId;
        private int replyBudget;
        private int replyPadding;

        KafkaCacheServerFetchStream(
            KafkaCacheServerFetchFanout group,
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

        private void onInitial(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onInitialBegin(begin);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onInitialEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onInitialAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onReplyWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReplyReset(reset);
                break;
            default:
                break;
            }
        }

        private void onInitialBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();

            state = KafkaState.openingInitial(state);

            group.onMemberOpening(traceId, this);
        }

        private void onInitialEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedInitial(state);

            group.onMemberClosed(traceId, this);

            doReplyEndIfNecessary(traceId);
        }

        private void onInitialAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedInitial(state);

            group.onMemberClosed(traceId, this);

            doReplyAbortIfNecessary(traceId);
        }

        private void onReplyWindow(
            WindowFW window)
        {
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            replyBudgetId = budgetId;
            replyBudget += credit;
            replyPadding = padding;

            if (!KafkaState.replyOpened(state))
            {
                state = KafkaState.openedReply(state);

                final long traceId = window.traceId();
                group.onMemberOpened(traceId, this);
            }
        }

        private void onReplyReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedInitial(state);

            group.onMemberClosed(traceId, this);

            doInitialResetIfNecessary(traceId);
        }

        private void doReplyBeginIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyOpening(state))
            {
                doReplyBegin(traceId);
            }
        }

        private void doReplyBegin(
            long traceId)
        {
            state = KafkaState.openingReply(state);

            router.setThrottle(replyId, this::onInitial);
            doBegin(sender, routeId, replyId, traceId, authorization, affinity,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .meta(m -> m.topic(group.topic))
                        .build()
                        .sizeof()));
        }

        private void doReplyData(
            long traceId)
        {
            // flush cache topic partition segment
        }

        private void doReplyEnd(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            doEnd(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doReplyAbort(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            doAbort(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doInitialWindowIfNecessary(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            if (!KafkaState.initialOpened(state) || credit > 0)
            {
                doInitialWindow(traceId, budgetId, credit, padding);
            }
        }

        private void doInitialWindow(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            doWindow(sender, routeId, initialId, traceId, authorization,
                    budgetId, credit, padding);

            state = KafkaState.openedInitial(state);
        }

        private void doInitialReset(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doReset(sender, routeId, initialId, traceId, authorization);
        }

        private void doReplyEndIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doReplyEnd(traceId);
            }
        }

        private void doReplyAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doReplyAbort(traceId);
            }
        }

        private void doInitialResetIfNecessary(
            long traceId)
        {
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doInitialReset(traceId);
            }
        }
    }
}
