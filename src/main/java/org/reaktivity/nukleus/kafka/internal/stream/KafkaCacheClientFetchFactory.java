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

import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheClusterReader;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartitionReader;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheReader;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheTopicReader;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheWriter;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaFilterFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheKeyFW;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaCacheClientFetchFactory implements StreamFactory
{
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};

    private static final int FLAG_FIN = 0x01;
    private static final int FLAG_INIT = 0x02;
    private static final int FLAG_NONE = 0x00;

    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final FlushFW flushRO = new FlushFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();
    private final KafkaCacheBeginExFW kafkaCacheBeginExRO = new KafkaCacheBeginExFW();
    private final KafkaCacheFlushExFW kafkaCacheFlushExRO = new KafkaCacheFlushExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();
    private final KafkaDataExFW.Builder kafkaDataExRW = new KafkaDataExFW.Builder();
    private final KafkaCacheBeginExFW.Builder kafkaCacheBeginExRW = new KafkaCacheBeginExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final int kafkaTypeId;
    private final int kafkaCacheTypeId;
    private final KafkaCacheReader cacheReader;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final BufferPool bufferPool;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongFunction<BudgetDebitor> supplyDebitor;
    private final LongFunction<KafkaCacheRoute> supplyCacheRoute;
    private final Long2ObjectHashMap<MessageConsumer> correlations;

    public KafkaCacheClientFetchFactory(
        KafkaConfiguration config,
        KafkaCacheReader cacheReader,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        LongFunction<BudgetDebitor> supplyDebitor,
        LongFunction<KafkaCacheRoute> supplyCacheRoute,
        Long2ObjectHashMap<MessageConsumer> correlations)
    {
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.kafkaCacheTypeId = supplyTypeId.applyAsInt(KafkaCacheWriter.TYPE_NAME);
        this.cacheReader = cacheReader;
        this.router = router;
        this.writeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = bufferPool;
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.supplyDebitor = supplyDebitor;
        this.supplyCacheRoute = supplyCacheRoute;
        this.correlations = correlations;
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
        assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_FETCH;
        final KafkaFetchBeginExFW kafkaFetchBeginEx = kafkaBeginEx.fetch();
        final String16FW beginTopic = kafkaFetchBeginEx.topic();
        final ArrayFW<KafkaOffsetFW> progress = kafkaFetchBeginEx.progress();
        final ArrayFW<KafkaFilterFW> filters = kafkaFetchBeginEx.filters();
        final KafkaOffsetFW progressItem = progress.matchFirst(pi -> true);

        MessageConsumer newStream = null;

        if (filters.isEmpty() &&
            progressItem != null && progressItem.limit() == progress.limit())
        {
            final MessagePredicate filter = (t, b, i, l) ->
            {
                final RouteFW route = wrapRoute.apply(t, b, i, l);
                final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
                final String16FW routeTopic = routeEx != null ? routeEx.topic() : null;
                return !route.localAddress().equals(route.remoteAddress()) &&
                        routeTopic != null && Objects.equals(routeTopic, beginTopic);
            };

            final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
            if (route != null)
            {
                final long resolvedId = route.correlationId();
                final String topicName = beginTopic.asString();
                final int partitionId = progressItem.partitionId();
                final long partitionOffset = progressItem.offset$();
                final KafkaCacheRoute cacheRoute = supplyCacheRoute.apply(resolvedId);
                final long partitionKey = cacheRoute.topicPartitionKey(topicName, partitionId);
                KafkaCacheClientFetchFanout fanout = cacheRoute.clientFetchFanoutsByTopicPartition.get(partitionKey);
                if (fanout == null)
                {
                    final String clusterName = route.remoteAddress().asString();
                    final KafkaCacheClusterReader cluster = cacheReader.supplyCluster(clusterName);
                    final KafkaCacheTopicReader topic = cluster.supplyTopic(topicName);
                    final KafkaCachePartitionReader partition = topic.supplyPartition(partitionId);
                    final KafkaCacheClientFetchFanout newFanout =
                            new KafkaCacheClientFetchFanout(resolvedId, authorization, topic, partition);

                    cacheRoute.clientFetchFanoutsByTopicPartition.put(partitionKey, newFanout);
                    fanout = newFanout;
                }

                if (fanout != null)
                {
                    newStream = new KafkaCacheClientFetchStream(
                            fanout,
                            sender,
                            routeId,
                            initialId,
                            affinity,
                            authorization,
                            partitionOffset)::onClientMessage;
                }
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
        Consumer<OctetsFW.Builder> payload,
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

    final class KafkaCacheClientFetchFanout
    {
        private final long routeId;
        private final long authorization;
        private final KafkaCacheTopicReader topic;
        private final KafkaCachePartitionReader partition;
        private final List<KafkaCacheClientFetchStream> members;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private long progressOffset;

        private KafkaCacheClientFetchFanout(
            long routeId,
            long authorization,
            KafkaCacheTopicReader topic,
            KafkaCachePartitionReader partition)
        {
            this.routeId = routeId;
            this.authorization = authorization;
            this.topic = topic;
            this.partition = partition;
            this.progressOffset = partition.progressOffset();
            this.members = new ArrayList<>();
        }

        private void onClientFanoutMemberOpening(
            long traceId,
            KafkaCacheClientFetchStream member)
        {
            members.add(member);

            assert !members.isEmpty();

            doClientFanoutInitialBeginIfNecessary(traceId);

            if (KafkaState.initialOpened(state))
            {
                member.doClientInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }

            if (KafkaState.replyOpened(state))
            {
                member.doClientReplyBeginIfNecessary(traceId);
            }
        }

        private void onClientFanoutMemberOpened(
            long traceId,
            KafkaCacheClientFetchStream member)
        {
            if (KafkaState.replyOpened(state))
            {
                member.doClientReplyDataIfNecessary(traceId);
            }
        }

        private void onClientFanoutMemberClosed(
            long traceId,
            KafkaCacheClientFetchStream member)
        {
            members.remove(member);

            if (members.isEmpty())
            {
                correlations.remove(replyId);
                doClientFanoutInitialEndIfNecessary(traceId);
            }
        }

        private void doClientFanoutInitialBeginIfNecessary(
            long traceId)
        {
            if (KafkaState.initialClosed(state) &&
                KafkaState.replyClosed(state))
            {
                state = 0;
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
            doBegin(receiver, routeId, initialId, traceId, authorization, 0L,
                ex -> ex.set((b, o, l) -> kafkaCacheBeginExRW.wrap(b, o, l)
                        .typeId(kafkaCacheTypeId)
                        .topic(topic.name())
                        .partitionId(partition.id())
                        .progressOffset(progressOffset)
                        .build()
                        .sizeof()));
            state = KafkaState.openingInitial(state);
        }

        private void doClientFanoutInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doClientFanoutInitialEnd(traceId);
            }
        }

        private void doClientFanoutInitialEnd(
            long traceId)
        {
            doEnd(receiver, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);

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
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onClientFanoutReplyFlush(flush);
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
            final KafkaCacheBeginExFW kafkaCacheBeginEx = extension.get(kafkaCacheBeginExRO::wrap);
            final String topicName = kafkaCacheBeginEx.topic().asString();
            final int partitionId = kafkaCacheBeginEx.partitionId();
            final long progressOffset = kafkaCacheBeginEx.progressOffset();

            state = KafkaState.openedReply(state);

            assert topic.name().equals(topicName);
            assert partitionId == partition.id();
            assert progressOffset >= 0 && progressOffset >= this.progressOffset;
            this.progressOffset = progressOffset;

            members.forEach(s -> s.doClientReplyBeginIfNecessary(traceId));

            doClientFanoutReplyWindow(traceId, bufferPool.slotCapacity());
        }

        private void onClientFanoutReplyFlush(
            FlushFW flush)
        {
            final long traceId = flush.traceId();
            final OctetsFW extension = flush.extension();
            final KafkaCacheFlushExFW kafkaCacheFlushEx = extension.get(kafkaCacheFlushExRO::wrap);
            final long progressOffset = kafkaCacheFlushEx.progressOffset();

            assert progressOffset > this.progressOffset;
            this.progressOffset = progressOffset;

            members.forEach(s -> s.doClientReplyDataIfNecessary(traceId));
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

            members.forEach(s -> s.doReplyAbortIfNecessary(traceId));

            state = KafkaState.closedReply(state);
        }

        private void onClientFanoutInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            members.forEach(s -> s.doInitialResetIfNecessary(traceId));

            state = KafkaState.closedInitial(state);

            doClientFanoutReplyResetIfNecessary(traceId);
        }

        private void onClientFanoutInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                members.forEach(s -> s.doClientInitialWindowIfNecessary(traceId, 0L, 0, 0));
            }
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

            doReset(receiver, routeId, replyId, traceId, authorization);
        }

        private void doClientFanoutReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            doWindow(receiver, routeId, replyId, traceId, authorization, 0L, credit, 0);
        }
    }

    private final class KafkaCacheClientFetchStream
    {
        private final KafkaCacheClientFetchFanout group;
        private final MessageConsumer sender;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long authorization;

        private int state;

        private long replyDebitorIndex = NO_DEBITOR_INDEX;
        private BudgetDebitor replyDebitor;

        private long replyBudgetId;
        private int replyBudget;
        private int replyPadding;

        private long progressOffset;
        private int messageOffset;

        KafkaCacheClientFetchStream(
            KafkaCacheClientFetchFanout group,
            MessageConsumer sender,
            long routeId,
            long initialId,
            long affinity,
            long authorization,
            long partitionOffset)
        {
            this.group = group;
            this.sender = sender;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.affinity = affinity;
            this.authorization = authorization;
            this.progressOffset = partitionOffset;
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

            doReplyAbortIfNecessary(traceId);
        }

        private void doClientInitialWindowIfNecessary(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            if (!KafkaState.initialOpened(state) || credit > 0)
            {
                doClientInitialWindow(traceId, budgetId, credit, padding);
            }
        }

        private void doClientInitialWindow(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            state = KafkaState.openedInitial(state);

            doWindow(sender, routeId, initialId, traceId, authorization,
                    budgetId, credit, padding);
        }

        private void doInitialResetIfNecessary(
                long traceId)
        {
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doClientInitialReset(traceId);
            }
        }

        private void doClientInitialReset(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doReset(sender, routeId, initialId, traceId, authorization);
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

            this.progressOffset = Math.max(progressOffset, group.progressOffset);

            router.setThrottle(replyId, this::onClientMessage);
            doBegin(sender, routeId, replyId, traceId, authorization, affinity,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.topic(group.topic.name())
                                     .progressItem(pi -> pi.partitionId(group.partition.id())
                                                           .offset$(progressOffset)))
                        .build()
                        .sizeof()));
        }

        private void doClientReplyDataIfNecessary(
            long traceId)
        {
            if (progressOffset < group.progressOffset)
            {
                doClientReplyData(traceId);
            }
        }

        private void doClientReplyData(
            long traceId)
        {
            final KafkaCacheEntryFW entry = group.partition.readEntry(progressOffset);
            if (entry != null)
            {
                final long partitionOffset = entry.offset$();
                final long timestamp = entry.timestamp();
                final KafkaCacheKeyFW key = entry.key();
                final ArrayFW<KafkaCacheHeaderFW> headers = entry.headers();
                final OctetsFW value = entry.value();
                final int remaining = value.sizeof() - messageOffset;
                final int lengthMin = Math.min(remaining, 1024);
                final int reservedMax = remaining + replyPadding;
                final int reservedMin = lengthMin + replyPadding;

                assert partitionOffset >= progressOffset;

                flush:
                if (replyBudget >= reservedMin)
                {
                    int reserved = reservedMax;
                    if (replyDebitorIndex != NO_DEBITOR_INDEX)
                    {
                        reserved = replyDebitor.claim(replyDebitorIndex, replyId, reservedMin, reservedMax);
                    }

                    if (reserved == 0)
                    {
                        break flush;
                    }

                    final int length = reserved - replyPadding;

                    int flags = 0x00;
                    if (messageOffset == 0)
                    {
                        flags |= FLAG_INIT;
                    }
                    if (length == remaining)
                    {
                        flags |= FLAG_FIN;
                    }

                    final int partitionId = group.partition.id();
                    final long progressOffset = partitionOffset + 1;
                    switch (flags)
                    {
                    case FLAG_INIT | FLAG_FIN:
                        doData(sender, routeId, replyId, traceId, authorization, flags, replyBudgetId, reserved,
                            p -> p.set(value),
                            ex -> ex.set((b, o, l) -> kafkaDataExRW.wrap(b, o, l)
                                    .typeId(kafkaTypeId)
                                    .fetch(f -> f.timestamp(timestamp)
                                                 .key(k -> k.value(key.value()))
                                                 .headers(hs -> headers.forEach(h -> hs.item(i -> i.name(h.name())
                                                                                                   .value(h.value()))))
                                                 .progressItem(pi -> pi.partitionId(partitionId)
                                                                       .offset$(progressOffset)))
                                    .build()
                                    .sizeof()));
                        break;
                    case FLAG_INIT:
                        doData(sender, routeId, replyId, traceId, authorization, flags, replyBudgetId, reserved,
                            p -> p.set(value.buffer(), value.offset() + messageOffset, length),
                            ex -> ex.set((b, o, l) -> kafkaDataExRW.wrap(b, o, l)
                                    .typeId(kafkaTypeId)
                                    .fetch(f -> f.timestamp(timestamp)
                                                 .key(k -> k.value(key.value())))
                                    .build()
                                    .sizeof()));
                        break;
                    case FLAG_NONE:
                        doData(sender, routeId, replyId, traceId, authorization, flags, replyBudgetId, reserved,
                            p -> p.set(value.buffer(), value.offset() + messageOffset, length),
                            EMPTY_EXTENSION);
                        break;
                    case FLAG_FIN:
                        doData(sender, routeId, replyId, traceId, authorization, flags, replyBudgetId, reserved,
                            p -> p.set(value.buffer(), value.offset() + messageOffset, length),
                            ex -> ex.set((b, o, l) -> kafkaDataExRW.wrap(b, o, l)
                                    .typeId(kafkaTypeId)
                                    .fetch(f -> f.headers(hs -> headers.forEach(h -> hs.item(i -> i.name(h.name())
                                                                                                   .value(h.value()))))
                                                 .progressItem(pi -> pi.partitionId(partitionId)
                                                                       .offset$(progressOffset)))
                                    .build()
                                    .sizeof()));
                        break;
                    }

                    if ((flags & FLAG_FIN) == 0x00)
                    {
                        this.messageOffset += length;
                    }
                    else
                    {
                        this.messageOffset = 0;
                        this.progressOffset = progressOffset;
                    }
                }
            }
        }

        private void doClientReplyEnd(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            doEnd(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
            doCleanupDebitorIfNecessary();
        }

        private void doClientReplyAbort(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            doAbort(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
            doCleanupDebitorIfNecessary();
        }

        private void doClientReplyEndIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doClientReplyEnd(traceId);
            }
        }

        private void doReplyAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doClientReplyAbort(traceId);
            }
        }

        private void onClientReplyWindow(
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

                if (replyBudgetId != 0L && replyDebitorIndex == NO_DEBITOR_INDEX)
                {
                    replyDebitor = supplyDebitor.apply(replyBudgetId);
                    replyDebitorIndex = replyDebitor.acquire(replyBudgetId, replyId, this::doClientReplyDataIfNecessary);
                }

                final long traceId = window.traceId();
                group.onClientFanoutMemberOpened(traceId, this);
            }
        }

        private void onClientReplyReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedReply(state);
            doCleanupDebitorIfNecessary();

            group.onClientFanoutMemberClosed(traceId, this);

            doInitialResetIfNecessary(traceId);
        }

        private void doCleanupDebitorIfNecessary()
        {
            if (replyDebitor != null && replyDebitorIndex != NO_DEBITOR_INDEX)
            {
                replyDebitor.release(replyBudgetId, replyId);
                replyDebitorIndex = NO_DEBITOR_INDEX;
            }
        }
    }
}
